package api

import (
	"cmp"
	"errors"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
)

const (
	legacySyncBatchRetention   = 5 * time.Minute
	legacySyncMaxBatchesPerUID = 256
)

var (
	errLegacySyncAckBusy     = errors.New("同步回执正在处理，请稍后重试")
	errLegacySyncPullExpired = errors.New("同步请求已过期，请重新同步")
)

type syncRecord struct {
	channelId   string
	channelType uint8
	lastMsgSeq  uint64
}

func legacySyncSortedChannels(channels []*channelRecentMessage) []*channelRecentMessage {
	// 远端和本地分片并行返回，旧版补拉需要固定频道顺序才能稳定地重发同一页。
	channels = slices.Clone(channels)
	slices.SortStableFunc(channels, func(a, b *channelRecentMessage) int {
		if a == nil || b == nil {
			switch {
			case a == b:
				return 0
			case a == nil:
				return 1
			default:
				return -1
			}
		}
		return cmp.Or(cmp.Compare(a.ChannelId, b.ChannelId), cmp.Compare(a.ChannelType, b.ChannelType))
	})
	return channels
}

// legacySyncStart 使用包含起点的正序查询，先跳过网关不可见的历史前缀。
func legacySyncStart(readTo, visibleAfter uint64) (uint64, bool) {
	after := max(readTo, visibleAfter)
	if after >= wkdb.MaxMessageSequence {
		return 0, false
	}
	return after + 1, true
}

type legacySyncCutoff struct {
	channelID   string
	channelType uint8
	afterSeq    uint64
}

func (v *legacySyncVisibility) forStorage(opts *options.Options) legacySyncCutoff {
	// 普通频道和命令频道分别分配序号。只有显式的 CMD 存储键能在补拉时形成历史下界。
	if !v.valid() || !opts.IsCmdChannel(v.ChannelID) {
		return legacySyncCutoff{}
	}
	return legacySyncCutoff{v.ChannelID, v.ChannelType, v.AfterSeq}
}

func (v legacySyncCutoff) after(channelID string, channelType uint8) uint64 {
	if channelID == v.channelID && channelType == v.channelType {
		return v.afterSeq
	}
	return 0
}

func legacySyncQueries(conversations []wkdb.Conversation, visibility legacySyncCutoff) []*channelRecentMessageReq {
	var requests []*channelRecentMessageReq
	for _, conversation := range conversations {
		start, ok := legacySyncStart(conversation.ReadToMsgSeq, visibility.after(conversation.ChannelId, conversation.ChannelType))
		if ok {
			requests = append(requests, &channelRecentMessageReq{
				ChannelId:   conversation.ChannelId,
				ChannelType: conversation.ChannelType,
				LastMsgSeq:  start,
			})
		}
	}
	return requests
}

func writeLegacySyncResponse(c *wkhttp.Context, messages []*types.MessageResp, scoped bool) {
	if messages == nil {
		messages = []*types.MessageResp{}
	}
	if scoped {
		// 对象表示 IM 已按序号域处理可见范围；旧调用继续收到数组。
		c.JSON(http.StatusOK, struct {
			Messages []*types.MessageResp `json:"messages"`
		}{Messages: messages})
		return
	}
	c.JSON(http.StatusOK, messages)
}

// legacySyncPage 只登记本次真正返回给客户端的各频道消息水位。
func legacySyncPage(channels []*channelRecentMessage, limit int, visibility legacySyncCutoff) ([]*types.MessageResp, []syncRecord) {
	messages := make([]*types.MessageResp, 0)
	var records []syncRecord
	for _, channel := range channels {
		if len(messages) >= limit {
			break
		}
		if channel == nil {
			continue
		}
		start := len(messages)
		visibleAfter := visibility.after(channel.ChannelId, channel.ChannelType)
		for _, message := range channel.Messages {
			if len(messages) >= limit {
				break
			}
			if message != nil && message.MessageSeq > visibleAfter {
				messages = append(messages, message)
			}
		}
		if len(messages) > start {
			records = append(records, syncRecord{
				channelId:   channel.ChannelId,
				channelType: channel.ChannelType,
				lastMsgSeq:  messages[len(messages)-1].MessageSeq,
			})
		}
	}
	return messages, records
}

type legacySyncBatch struct {
	records   []syncRecord
	expiresAt time.Time
	acked     bool
	conflict  legacySyncProtection
}

type legacyUserSyncBatches struct {
	byLastSeq map[uint64]*legacySyncBatch
	applying  *legacySyncBatch
	untracked legacySyncProtection
}

// 同一快照重发时不推迟它自身恢复确认，但要继续保护它不被误认成另一快照。
// 只保留最近返回的一个快照；它变化时，需重新等候此前其他快照的迟到回执。
type legacySyncProtection struct {
	lastSeq   uint64
	records   []syncRecord
	cleanAt   time.Time
	expiresAt time.Time
}

func (p *legacySyncProtection) matches(seq uint64, records []syncRecord) bool {
	return seq == p.lastSeq && slices.Equal(p.records, records)
}

func (p *legacySyncProtection) observe(seq uint64, records []syncRecord, now time.Time) {
	expiresAt := now.Add(legacySyncBatchRetention)
	if !p.matches(seq, records) {
		p.lastSeq, p.records, p.cleanAt = seq, records, expiresAt
	}
	p.expiresAt = expiresAt
}

// legacySyncBatches 只在进程内短期保留批次。旧协议只回传频道内序号，超出保护期或重启后
// 无法区分复用同一序号的迟到回执；保护期内仍返回冲突页，但不凭有歧义的回执推进游标。
type legacySyncBatches struct {
	mu        sync.Mutex
	users     map[string]*legacyUserSyncBatches
	nextSweep time.Time
}

func (s *legacySyncBatches) publish(uid string, channels []*channelRecentMessage, limit int, visibility legacySyncCutoff, now time.Time) []*types.MessageResp {
	ordered := legacySyncSortedChannels(channels)
	messages, records := legacySyncPage(ordered, limit, visibility)
	if len(messages) == 0 {
		return messages
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// 用登记顺序计时，避免排队的旧请求缩短较新页面的保护期；单测可固定时间。
	if now.IsZero() {
		now = time.Now()
	}
	s.sweep(now)
	user := s.users[uid]
	if user == nil {
		user = &legacyUserSyncBatches{byLastSeq: make(map[uint64]*legacySyncBatch)}
		if s.users == nil {
			s.users = make(map[string]*legacyUserSyncBatches)
		}
		s.users[uid] = user
	}
	if len(user.byLastSeq) >= legacySyncMaxBatchesPerUID {
		user.prune(now)
	}
	lastSeq := messages[len(messages)-1].MessageSeq
	old := user.active(lastSeq, now)
	if old != nil {
		if old.conflict.lastSeq == 0 && slices.Equal(old.records, records) {
			// 同一页的迟到回执仍指向相同水位，可以安全重试。
			old.acked = false
			old.expiresAt = now.Add(legacySyncBatchRetention)
			return messages
		}
		if old != user.applying && old.conflict.matches(lastSeq, records) && !now.Before(old.conflict.cleanAt) {
			user.register(lastSeq, records, now)
			return messages
		}
	}
	if !now.Before(user.untracked.cleanAt) && len(user.byLastSeq) < legacySyncMaxBatchesPerUID {
		if old == nil && user.canRegisterUntracked(lastSeq, records, now) {
			user.register(lastSeq, records, now)
			return messages
		}
		// 优先保留正常页的最长可确认前缀，不能登记被裁掉的消息。
		for i := len(messages) - 2; i >= 0; i-- {
			seq := messages[i].MessageSeq
			if user.active(seq, now) == nil && user.canRegisterUntracked(seq, nil, now) {
				prefix, bounds := legacySyncPage(ordered, i+1, visibility)
				user.register(seq, bounds, now)
				return prefix
			}
		}
		// 若开头频道没有可区分的回执，让其他频道继续补拉。只选择该频道的连续前缀。
		for _, channel := range ordered {
			if channel == nil {
				continue
			}
			var prefix []*types.MessageResp
			visibleAfter := visibility.after(channel.ChannelId, channel.ChannelType)
			for _, message := range channel.Messages {
				if message == nil || message.MessageSeq <= visibleAfter {
					continue
				}
				prefix = append(prefix, message)
				if user.active(message.MessageSeq, now) == nil && user.canRegisterUntracked(message.MessageSeq, nil, now) {
					user.register(message.MessageSeq, []syncRecord{{channel.ChannelId, channel.ChannelType, message.MessageSeq}}, now)
					return prefix
				}
				if len(prefix) == limit {
					break
				}
			}
		}
	}
	if old != nil {
		old.conflict.observe(lastSeq, records, now)
	} else {
		// 容量满时不驱逐旧标记；未登记的旧序号在保护期内也不能绑定到其他页。
		user.untracked.observe(lastSeq, records, now)
	}
	return messages
}

func (user *legacyUserSyncBatches) canRegisterUntracked(seq uint64, records []syncRecord, now time.Time) bool {
	return seq != user.untracked.lastSeq || !now.Before(user.untracked.expiresAt) || user.untracked.matches(seq, records)
}

func (user *legacyUserSyncBatches) active(lastSeq uint64, now time.Time) *legacySyncBatch {
	batch := user.byLastSeq[lastSeq]
	if batch != nil && (batch == user.applying || now.Before(batch.expiresAt) || now.Before(batch.conflict.expiresAt)) {
		return batch
	}
	return nil
}

func (user *legacyUserSyncBatches) register(lastSeq uint64, records []syncRecord, now time.Time) {
	user.byLastSeq[lastSeq] = &legacySyncBatch{records: records, expiresAt: now.Add(legacySyncBatchRetention)}
}

func (s *legacySyncBatches) beginAck(uid string, lastSeq uint64, now time.Time) (*legacySyncBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweep(now)
	user := s.users[uid]
	if user == nil {
		return nil, nil
	}
	batch := user.active(lastSeq, now)
	if batch == nil || batch.acked || batch.conflict.lastSeq != 0 {
		return nil, nil
	}
	// 同一用户的回执串行落库，但不在查库、写库期间持有全局内存锁。
	if user.applying != nil {
		return nil, errLegacySyncAckBusy
	}
	user.applying = batch
	return batch, nil
}

func (s *legacySyncBatches) finishAck(uid string, batch *legacySyncBatch, success bool, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	user := s.users[uid]
	if user == nil || user.applying != batch {
		return
	}
	user.applying = nil
	if batch.conflict.lastSeq != 0 {
		return
	}
	batch.acked = success
	if expiresAt := now.Add(legacySyncBatchRetention); expiresAt.After(batch.expiresAt) {
		batch.expiresAt = expiresAt
	}
}

// sweep 必须持有 mu。清理闲置用户，不保留永久的回执状态或互斥锁。
func (s *legacySyncBatches) sweep(now time.Time) {
	if now.Before(s.nextSweep) {
		return
	}
	s.nextSweep = now.Add(time.Minute)
	for uid, user := range s.users {
		user.prune(now)
		if len(user.byLastSeq) == 0 && user.untracked.lastSeq == 0 {
			delete(s.users, uid)
		}
	}
}

func (user *legacyUserSyncBatches) prune(now time.Time) {
	for seq := range user.byLastSeq {
		if user.active(seq, now) == nil {
			delete(user.byLastSeq, seq)
		}
	}
	if !now.Before(user.untracked.expiresAt) {
		user.untracked = legacySyncProtection{}
	}
}
