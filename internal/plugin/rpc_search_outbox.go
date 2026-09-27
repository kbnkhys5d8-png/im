package plugin

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/wkrpc"
)

const (
	searchOutboxProtocolVersion = 2
	searchOutboxMaxAckCount     = 500
)

var (
	errSearchOutboxVersion           = errors.New("search outbox protocol version is invalid")
	errSearchOutboxLimit             = errors.New("search outbox pull limit is invalid")
	errSearchOutboxByteLimit         = errors.New("search outbox byte budget is invalid")
	errSearchOutboxNode              = errors.New("search outbox node id is invalid")
	errSearchOutboxAckCount          = errors.New("search outbox ack count is invalid")
	errSearchOutboxDuplicateIdentity = errors.New("search outbox ack identity is duplicated")
	errSearchOutboxApplied           = errors.New("search outbox record is not durably applied")
	errSearchOutboxTimestamp         = errors.New("search outbox record timestamp is invalid")
	errSearchOutboxIdentity          = errors.New("search outbox record identity is inconsistent")
	errSearchOutboxStore             = errors.New("search outbox store is unavailable")
)

type searchOutboxStore interface {
	PullSearchOutbox(
		limit int,
		maxBytes uint64,
	) (wkdb.SearchOutboxPullResult, error)
	AckSearchOutboxTasks([]wkdb.SearchOutboxTask) error
	QuarantineSearchOutbox(wkdb.SearchOutboxTask, string) error
	RestoreSearchOutbox(wkdb.SearchOutboxTask) (uint64, error)
}

type liveSearchOutboxStore struct{}

var _ searchOutboxStore = liveSearchOutboxStore{}

func defaultSearchOutboxStore() searchOutboxStore {
	return liveSearchOutboxStore{}
}

func (liveSearchOutboxStore) PullSearchOutbox(
	limit int,
	maxBytes uint64,
) (wkdb.SearchOutboxPullResult, error) {
	if service.Store == nil {
		return wkdb.SearchOutboxPullResult{}, errSearchOutboxStore
	}
	return service.Store.DB().PullSearchOutbox(limit, maxBytes)
}

func (liveSearchOutboxStore) AckSearchOutboxTasks(tasks []wkdb.SearchOutboxTask) error {
	store, err := liveSearchOutboxTaskStore()
	if err != nil {
		return err
	}
	return store.AckSearchOutboxTasks(tasks)
}

func (liveSearchOutboxStore) QuarantineSearchOutbox(task wkdb.SearchOutboxTask, reason string) error {
	store, err := liveSearchOutboxTaskStore()
	if err != nil {
		return err
	}
	return store.QuarantineSearchOutbox(task, reason)
}

func (liveSearchOutboxStore) RestoreSearchOutbox(task wkdb.SearchOutboxTask) (uint64, error) {
	store, err := liveSearchOutboxTaskStore()
	if err != nil {
		return 0, err
	}
	return store.RestoreSearchOutbox(task)
}

func liveSearchOutboxTaskStore() (wkdb.SearchOutboxTaskStore, error) {
	if service.Store == nil {
		return nil, errSearchOutboxStore
	}
	store, ok := service.Store.DB().(wkdb.SearchOutboxTaskStore)
	if !ok {
		return nil, errSearchOutboxStore
	}
	return store, nil
}

type searchOutboxPullRequest struct {
	Version  int    `json:"version"`
	Limit    int    `json:"limit"`
	MaxBytes uint64 `json:"max_bytes"`
}

type searchOutboxMessage struct {
	MessageID   int64  `json:"message_id"`
	MessageSeq  uint64 `json:"message_seq"`
	ClientMsgNo string `json:"client_msg_no"`
	StreamNo    string `json:"stream_no"`
	Timestamp   uint32 `json:"timestamp"`
	FromUID     string `json:"from_uid"`
	ChannelID   string `json:"channel_id"`
	ChannelType uint8  `json:"channel_type"`
	Topic       string `json:"topic"`
	Payload     []byte `json:"payload"`
}

type searchOutboxRPCRecord struct {
	Identity          wkdb.SearchOutboxIdentity `json:"identity"`
	Epoch             uint64                    `json:"epoch"`
	Message           searchOutboxMessage       `json:"message"`
	AppliedMessageSeq uint64                    `json:"applied_message_seq"`
}

type searchOutboxPullResponse struct {
	Version         int                     `json:"version"`
	NodeID          uint64                  `json:"node_id"`
	Pending         uint64                  `json:"pending"`
	Quarantined     uint64                  `json:"quarantined"`
	OldestCreatedAt int64                   `json:"oldest_created_at"`
	AppliedBlocked  uint64                  `json:"applied_blocked"`
	Records         []searchOutboxRPCRecord `json:"records"`
}

type searchOutboxAckRequest struct {
	Version    int                         `json:"version"`
	NodeID     uint64                      `json:"node_id"`
	Identities []wkdb.SearchOutboxIdentity `json:"identities"`
	Tasks      []wkdb.SearchOutboxTask     `json:"tasks"`
}

type searchOutboxAckResponse struct {
	Version      int    `json:"version"`
	NodeID       uint64 `json:"node_id"`
	Acknowledged int    `json:"acknowledged"`
}

func (a *rpc) searchOutboxPullRoute(c *wkrpc.Context) {
	var req searchOutboxPullRequest
	if err := json.Unmarshal(c.Body(), &req); err != nil {
		c.WriteErr(fmt.Errorf("decode search outbox pull request: %w", err))
		return
	}
	resp, err := a.searchOutboxPull(req)
	if err != nil {
		c.WriteErr(err)
		return
	}
	a.writeSearchSourceJSON(c, resp)
}

func (a *rpc) searchOutboxAckRoute(c *wkrpc.Context) {
	var req searchOutboxAckRequest
	if err := json.Unmarshal(c.Body(), &req); err != nil {
		c.WriteErr(fmt.Errorf("decode search outbox ack request: %w", err))
		return
	}
	resp, err := a.searchOutboxAck(req)
	if err != nil {
		c.WriteErr(err)
		return
	}
	a.writeSearchSourceJSON(c, resp)
}

func (a *rpc) searchOutboxPull(
	req searchOutboxPullRequest,
) (searchOutboxPullResponse, error) {
	if err := a.requireSearchOutboxReady(); err != nil {
		return searchOutboxPullResponse{}, err
	}
	if req.Version != searchOutboxProtocolVersion {
		return searchOutboxPullResponse{}, errSearchOutboxVersion
	}
	if req.Limit < 1 || req.Limit > wkdb.MaxSearchOutboxPullLimit {
		return searchOutboxPullResponse{}, errSearchOutboxLimit
	}
	if req.MaxBytes == 0 {
		return searchOutboxPullResponse{}, errSearchOutboxByteLimit
	}
	nodeID, err := a.validSearchOutboxNodeID()
	if err != nil {
		return searchOutboxPullResponse{}, err
	}
	if a.searchOutboxStore == nil {
		return searchOutboxPullResponse{}, errSearchOutboxStore
	}

	result, err := a.searchOutboxStore.PullSearchOutbox(
		req.Limit,
		req.MaxBytes,
	)
	if err != nil {
		return searchOutboxPullResponse{}, err
	}
	response := searchOutboxPullResponse{
		Version:         searchOutboxProtocolVersion,
		NodeID:          nodeID,
		Pending:         result.Pending,
		Quarantined:     result.Quarantined,
		OldestCreatedAt: result.OldestCreatedAt,
		AppliedBlocked:  result.AppliedBlocked,
		Records:         make([]searchOutboxRPCRecord, 0, len(result.Records)),
	}
	for _, record := range result.Records {
		if err := validateSearchOutboxRecord(record); err != nil {
			return searchOutboxPullResponse{}, err
		}
		message, err := searchOutboxMessageFromDB(record.Message)
		if err != nil {
			return searchOutboxPullResponse{}, err
		}
		response.Records = append(response.Records, searchOutboxRPCRecord{
			Identity:          record.Identity,
			Epoch:             record.Epoch,
			Message:           message,
			AppliedMessageSeq: record.AppliedIndex,
		})
	}
	return response, nil
}

func (a *rpc) searchOutboxAck(
	req searchOutboxAckRequest,
) (searchOutboxAckResponse, error) {
	if err := a.requireSearchOutboxReady(); err != nil {
		return searchOutboxAckResponse{}, err
	}
	if req.Version != searchOutboxProtocolVersion {
		return searchOutboxAckResponse{}, errSearchOutboxVersion
	}
	nodeID, err := a.validSearchOutboxNodeID()
	if err != nil {
		return searchOutboxAckResponse{}, err
	}
	if req.NodeID == 0 || req.NodeID != nodeID {
		return searchOutboxAckResponse{}, errSearchOutboxNode
	}
	// v2 不能接受不带处理代次的旧身份列表，否则迟到的 ACK 会删除恢复后的任务。
	if len(req.Identities) != 0 || len(req.Tasks) == 0 ||
		len(req.Tasks) > searchOutboxMaxAckCount {
		return searchOutboxAckResponse{}, errSearchOutboxAckCount
	}
	seen := make(map[wkdb.SearchOutboxIdentity]struct{}, len(req.Tasks))
	for _, task := range req.Tasks {
		if err := validateSearchOutboxTask(task); err != nil {
			return searchOutboxAckResponse{}, err
		}
		if _, ok := seen[task.Identity]; ok {
			return searchOutboxAckResponse{}, errSearchOutboxDuplicateIdentity
		}
		seen[task.Identity] = struct{}{}
	}
	if a.searchOutboxStore == nil {
		return searchOutboxAckResponse{}, errSearchOutboxStore
	}
	if err := a.searchOutboxStore.AckSearchOutboxTasks(req.Tasks); err != nil {
		return searchOutboxAckResponse{}, err
	}
	return searchOutboxAckResponse{
		Version:      searchOutboxProtocolVersion,
		NodeID:       nodeID,
		Acknowledged: len(req.Tasks),
	}, nil
}

func (a *rpc) requireSearchOutboxReady() error {
	if a.searchOutboxReady == nil || a.searchOutboxReady() != nil {
		return errSearchOutboxUnavailable
	}
	return nil
}

func (a *rpc) validSearchOutboxNodeID() (uint64, error) {
	if a.searchOutboxNodeID == nil {
		return 0, errSearchOutboxNode
	}
	nodeID := a.searchOutboxNodeID()
	if nodeID == 0 {
		return 0, errSearchOutboxNode
	}
	return nodeID, nil
}

func validateSearchOutboxRecord(record wkdb.SearchOutboxRecord) error {
	if record.Epoch == 0 {
		return wkdb.ErrSearchOutboxEpochRequired
	}
	if err := record.Identity.Validate(); err != nil {
		return err
	}
	if record.Identity.MessageID != record.Message.MessageID ||
		record.Identity.MessageSeq != uint64(record.Message.MessageSeq) ||
		record.Identity.ChannelID != record.Message.ChannelID ||
		record.Identity.ChannelType != record.Message.ChannelType {
		return errSearchOutboxIdentity
	}
	if record.AppliedIndex < record.Identity.MessageSeq ||
		record.AppliedIndex > wkdb.MaxMessageSequence {
		return errSearchOutboxApplied
	}
	return nil
}

func validateSearchOutboxTask(task wkdb.SearchOutboxTask) error {
	if task.Epoch == 0 {
		return wkdb.ErrSearchOutboxEpochRequired
	}
	return task.Identity.Validate()
}

func searchOutboxMessageFromDB(
	message wkdb.Message,
) (searchOutboxMessage, error) {
	if message.Timestamp <= 0 {
		return searchOutboxMessage{}, errSearchOutboxTimestamp
	}
	return searchOutboxMessage{
		MessageID:   message.MessageID,
		MessageSeq:  uint64(message.MessageSeq),
		ClientMsgNo: message.ClientMsgNo,
		StreamNo:    message.StreamNo,
		Timestamp:   uint32(message.Timestamp),
		FromUID:     message.FromUID,
		ChannelID:   message.ChannelID,
		ChannelType: message.ChannelType,
		Topic:       message.Topic,
		Payload:     append([]byte(nil), message.Payload...),
	}, nil
}
