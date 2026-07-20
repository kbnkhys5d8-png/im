package plugin

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/WuKongIM/wkrpc"
	"go.uber.org/zap"
)

const (
	searchSourceProtocolVersion      = 5
	searchSourceDefaultChannelLimit  = 100
	searchSourceMaxChannelLimit      = 500
	searchSourceMaxMessageLimit      = 500
	searchSourceMaxChannelIDBytes    = 100
	searchSourceMessageLoadBytes     = 3 * 1024 * 1024
	searchSourceMessageResponseBytes = 4 * 1024 * 1024
	searchSourceResponseReserveBytes = 64
	searchSourceMaxNextSeq           = wkdb.MaxMessageSequence + 1
	searchSourceChannelReadAttempts  = 3

	searchSourceErrorKindChannelRetry = "channel_retry"
	searchSourceErrorCodeApplyPending = "apply_pending"
)

var (
	errSearchSourceVersion     = errors.New("search source protocol version is invalid")
	errSearchSourceRoster      = errors.New("search source requires authoritative local single-node roster")
	errSearchSourceChannelID   = errors.New("channel_id is required")
	errSearchSourceChannelType = errors.New("channel_type is invalid")
	errSearchSourceLimit       = errors.New("search source limit is invalid")
	errSearchSourceGeneration  = errors.New("expected channel generation is invalid")
	errSearchSourceSequence    = errors.New("search source message sequence is outside uint32 protocol range")
	errSearchSourceFence       = errors.New("search source channel configuration changed while reading")
)

type searchSourceStore interface {
	GetChannelClusterConfigs(afterID uint64, limit int) ([]wkdb.ChannelClusterConfig, error)
	GetAppliedMsgSeq(channelID string, channelType uint8) (uint64, error)
	GetLastMsgSeq(channelID string, channelType uint8) (uint64, error)
	LoadNextRangeSearchSourceMessages(channelID string, channelType uint8, start, end uint64, limit int, limitSize uint64) ([]wkdb.SearchSourceMessage, error)
}

type liveSearchSourceStore struct{}

func defaultSearchSourceStore() searchSourceStore { return liveSearchSourceStore{} }

func (liveSearchSourceStore) GetChannelClusterConfigs(afterID uint64, limit int) ([]wkdb.ChannelClusterConfig, error) {
	if service.Store == nil {
		return nil, errors.New("store is unavailable")
	}
	return service.Store.DB().GetChannelClusterConfigs(afterID, limit)
}

func (liveSearchSourceStore) GetAppliedMsgSeq(channelID string, channelType uint8) (uint64, error) {
	if service.Store == nil {
		return 0, errors.New("store is unavailable")
	}
	return service.Store.DB().GetChannelAppliedIndex(channelID, channelType)
}

func (liveSearchSourceStore) GetLastMsgSeq(channelID string, channelType uint8) (uint64, error) {
	if service.Store == nil {
		return 0, errors.New("store is unavailable")
	}
	seq, _, err := service.Store.DB().GetChannelLastMessageSeq(channelID, channelType)
	return seq, err
}

func (liveSearchSourceStore) LoadNextRangeSearchSourceMessages(channelID string, channelType uint8, start, end uint64, limit int, limitSize uint64) ([]wkdb.SearchSourceMessage, error) {
	if service.Store == nil {
		return nil, errors.New("store is unavailable")
	}
	return service.Store.DB().LoadNextRangeSearchSourceMessages(channelID, channelType, start, end, limit, limitSize)
}

func defaultSearchSourceNodeID() uint64 { return options.G.Cluster.NodeId }

func defaultSearchSourceRoster() ([]uint64, error) {
	if service.Cluster == nil {
		return nil, errors.New("cluster is unavailable")
	}
	nodes := service.Cluster.Nodes()
	ids := make([]uint64, 0, len(nodes))
	for _, node := range nodes {
		if node == nil || node.Id == 0 {
			return nil, errSearchSourceRoster
		}
		ids = append(ids, node.Id)
	}
	return ids, nil
}

func defaultSearchSourceAuthority(channelID string, channelType uint8) (wkdb.ChannelClusterConfig, error) {
	if service.Cluster == nil {
		return wkdb.EmptyChannelClusterConfig, errors.New("cluster is unavailable")
	}
	return service.Cluster.LoadOnlyChannelClusterConfig(channelID, channelType)
}

type searchSourceChannelPageRequest struct {
	Version int    `json:"version"`
	AfterID uint64 `json:"after_id"`
	Limit   int    `json:"limit"`
}

type searchSourceChannel struct {
	ConfigID                 uint64 `json:"config_id"`
	ChannelID                string `json:"channel_id"`
	ChannelType              uint8  `json:"channel_type"`
	LeaderID                 uint64 `json:"leader_id"`
	Term                     uint32 `json:"term"`
	ConfigVersion            uint64 `json:"config_version"`
	LastMessageSeq           uint64 `json:"last_message_seq"`
	AppliedMessageSeq        uint64 `json:"applied_message_seq"`
	PhysicalMessageSeq       uint64 `json:"physical_message_seq"`
	ApplyPending             bool   `json:"apply_pending"`
	OfflineBootstrapRequired bool   `json:"offline_bootstrap_required"`
}

type searchSourceChannelPageResponse struct {
	Version        int                   `json:"version"`
	NodeID         uint64                `json:"node_id"`
	ClusterNodeIDs []uint64              `json:"cluster_node_ids"`
	ScannedTo      uint64                `json:"scanned_to"`
	Channels       []searchSourceChannel `json:"channels"`
}

type searchSourceMessageRequest struct {
	Version               int    `json:"version"`
	ChannelID             string `json:"channel_id"`
	ChannelType           uint8  `json:"channel_type"`
	ExpectedLeaderID      uint64 `json:"expected_leader_id"`
	ExpectedTerm          uint32 `json:"expected_term"`
	ExpectedConfigVersion uint64 `json:"expected_config_version"`
	NextSeq               uint64 `json:"next_seq"`
	Limit                 int    `json:"limit"`
}

type searchSourceMessage struct {
	MessageID      int64  `json:"message_id"`
	MessageSeq     uint64 `json:"message_seq"`
	ClientMsgNo    string `json:"client_msg_no"`
	StreamNo       string `json:"stream_no"`
	Setting        uint8  `json:"setting"`
	Timestamp      int32  `json:"timestamp"`
	Expire         uint32 `json:"expire"`
	FromUID        string `json:"from_uid"`
	ChannelID      string `json:"channel_id"`
	ChannelType    uint8  `json:"channel_type"`
	Topic          string `json:"topic"`
	Payload        []byte `json:"payload"`
	NoPersist      bool   `json:"no_persist"`
	PayloadOmitted bool   `json:"payload_omitted"`
	StorageTerm    uint64 `json:"storage_term"`
}

type searchSourceMessageResponse struct {
	Version                  int                   `json:"version"`
	NodeID                   uint64                `json:"node_id"`
	ClusterNodeIDs           []uint64              `json:"cluster_node_ids"`
	ChannelID                string                `json:"channel_id"`
	ChannelType              uint8                 `json:"channel_type"`
	LeaderID                 uint64                `json:"leader_id"`
	Term                     uint32                `json:"term"`
	ConfigVersion            uint64                `json:"config_version"`
	LastMessageSeq           uint64                `json:"last_message_seq"`
	AppliedMessageSeq        uint64                `json:"applied_message_seq"`
	PhysicalMessageSeq       uint64                `json:"physical_message_seq"`
	ApplyPending             bool                  `json:"apply_pending"`
	OfflineBootstrapRequired bool                  `json:"offline_bootstrap_required"`
	NextSeq                  uint64                `json:"next_seq"`
	CaughtUp                 bool                  `json:"caught_up"`
	NotOwner                 bool                  `json:"not_owner"`
	Retryable                bool                  `json:"retryable"`
	ErrorKind                string                `json:"error_kind"`
	ErrorCode                string                `json:"error_code"`
	Messages                 []searchSourceMessage `json:"messages"`
}

func (a *rpc) searchSourceChannelsRoute(c *wkrpc.Context) {
	var req searchSourceChannelPageRequest
	if err := json.Unmarshal(c.Body(), &req); err != nil {
		c.WriteErr(fmt.Errorf("decode search source channel request: %w", err))
		return
	}
	resp, err := a.searchSourceChannels(req)
	if err != nil {
		a.Error("search source channel inventory failed", zap.Error(err))
		c.WriteErr(err)
		return
	}
	a.writeSearchSourceJSON(c, resp)
}

func (a *rpc) requireSearchSourceAuthorization(next wkrpc.Handler) wkrpc.Handler {
	return func(c *wkrpc.Context) {
		if a.s == nil || a.s.searchAuth == nil {
			c.WriteErr(errSearchSourceUnauthorized)
			return
		}
		if err := a.s.searchAuth.authorizeRequest(c.Conn().Fd(), c.Uid(), c.Conn()); err != nil {
			a.Warn("search source request rejected", zap.Error(err))
			c.WriteErr(errSearchSourceUnauthorized)
			return
		}
		if a.searchSourceReady == nil || a.searchSourceReady() != nil {
			c.WriteErr(errSearchSourceUnavailable)
			return
		}
		next(c)
	}
}

func (a *rpc) searchSourceMessagesRoute(c *wkrpc.Context) {
	var req searchSourceMessageRequest
	if err := json.Unmarshal(c.Body(), &req); err != nil {
		c.WriteErr(fmt.Errorf("decode search source message request: %w", err))
		return
	}
	resp, err := a.searchSourceMessages(req)
	if err != nil {
		a.Error("search source message read failed", zap.Error(err))
		c.WriteErr(err)
		return
	}
	a.writeSearchSourceJSON(c, resp)
}

func (a *rpc) writeSearchSourceJSON(c *wkrpc.Context, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		c.WriteErr(err)
		return
	}
	c.Write(data)
}

func (a *rpc) searchSourceChannels(req searchSourceChannelPageRequest) (searchSourceChannelPageResponse, error) {
	if a.searchSourceReady == nil {
		return searchSourceChannelPageResponse{}, errSearchSourceUnavailable
	}
	if err := a.searchSourceReady(); err != nil {
		return searchSourceChannelPageResponse{}, err
	}
	if req.Version != searchSourceProtocolVersion {
		return searchSourceChannelPageResponse{}, errSearchSourceVersion
	}
	limit, err := searchSourceChannelLimit(req.Limit)
	if err != nil {
		return searchSourceChannelPageResponse{}, err
	}
	nodeID, roster, err := a.validateSearchSourceRoster()
	if err != nil {
		return searchSourceChannelPageResponse{}, err
	}
	resp := searchSourceChannelPageResponse{
		Version: searchSourceProtocolVersion, NodeID: nodeID, ClusterNodeIDs: roster,
		ScannedTo: req.AfterID, Channels: make([]searchSourceChannel, 0),
	}
	configs, err := a.searchSourceStore.GetChannelClusterConfigs(req.AfterID, limit)
	if err != nil {
		return searchSourceChannelPageResponse{}, err
	}
	for _, candidate := range configs {
		if candidate.Id <= resp.ScannedTo || !validSearchSourceSingleNodeConfig(candidate, nodeID) {
			return searchSourceChannelPageResponse{}, errSearchSourceFence
		}
		resp.ScannedTo = candidate.Id
		channel, err := a.readStableSearchSourceChannel(candidate, nodeID)
		if err != nil {
			return searchSourceChannelPageResponse{}, err
		}
		resp.Channels = append(resp.Channels, channel)
	}
	if _, _, err := a.validateSearchSourceRoster(); err != nil {
		return searchSourceChannelPageResponse{}, err
	}
	return resp, nil
}

func (a *rpc) searchSourceMessages(req searchSourceMessageRequest) (searchSourceMessageResponse, error) {
	if a.searchSourceReady == nil {
		return searchSourceMessageResponse{}, errSearchSourceUnavailable
	}
	if err := a.searchSourceReady(); err != nil {
		return searchSourceMessageResponse{}, err
	}
	if err := validateSearchSourceMessageRequest(req); err != nil {
		return searchSourceMessageResponse{}, err
	}
	if req.NextSeq == 0 {
		req.NextSeq = 1
	}
	nodeID, roster, err := a.validateSearchSourceRoster()
	if err != nil {
		return searchSourceMessageResponse{}, err
	}
	resp := searchSourceMessageResponse{
		Version: searchSourceProtocolVersion, NodeID: nodeID, ClusterNodeIDs: roster,
		ChannelID: req.ChannelID, ChannelType: req.ChannelType,
		LeaderID: req.ExpectedLeaderID, Term: req.ExpectedTerm, ConfigVersion: req.ExpectedConfigVersion,
		NextSeq: req.NextSeq, Messages: make([]searchSourceMessage, 0),
	}
	before, err := a.searchSourceAuthority(req.ChannelID, req.ChannelType)
	if err != nil {
		return searchSourceMessageResponse{}, err
	}
	if !searchSourceOwns(before, nodeID, req) {
		return searchSourceNotOwner(resp, before), nil
	}
	applied, physical, err := a.searchSourceTail(req.ChannelID, req.ChannelType)
	if err != nil {
		return searchSourceMessageResponse{}, err
	}
	resp.LastMessageSeq = applied
	resp.AppliedMessageSeq = applied
	resp.PhysicalMessageSeq = physical
	resp.ApplyPending = physical > applied
	resp.OfflineBootstrapRequired = applied == 0 && physical > 0
	var stored []wkdb.SearchSourceMessage
	if !resp.ApplyPending && req.NextSeq <= applied {
		stored, err = a.searchSourceStore.LoadNextRangeSearchSourceMessages(req.ChannelID, req.ChannelType, req.NextSeq, applied+1, req.Limit, searchSourceMessageLoadBytes)
		if err != nil {
			return searchSourceMessageResponse{}, err
		}
		if err := validateSearchSourceStoredPage(req, applied, stored); err != nil {
			return searchSourceMessageResponse{}, err
		}
	}
	after, err := a.searchSourceAuthority(req.ChannelID, req.ChannelType)
	if err != nil {
		return searchSourceMessageResponse{}, err
	}
	if !searchSourceOwns(after, nodeID, req) || !before.Equal(after) {
		return searchSourceNotOwner(resp, after), nil
	}
	if _, _, err := a.validateSearchSourceRoster(); err != nil {
		return searchSourceMessageResponse{}, err
	}
	if resp.ApplyPending {
		resp.Retryable = true
		resp.ErrorKind = searchSourceErrorKindChannelRetry
		resp.ErrorCode = searchSourceErrorCodeApplyPending
		return resp, nil
	}
	resp.Messages = make([]searchSourceMessage, 0, len(stored))
	for _, row := range stored {
		resp.Messages = append(resp.Messages, searchSourceMessageFromDB(row))
	}
	if err := boundSearchSourceMessageResponse(&resp); err != nil {
		return searchSourceMessageResponse{}, err
	}
	if len(resp.Messages) > 0 {
		resp.NextSeq = resp.Messages[len(resp.Messages)-1].MessageSeq + 1
	}
	resp.CaughtUp = resp.NextSeq > applied
	return resp, nil
}

func (a *rpc) validateSearchSourceRoster() (uint64, []uint64, error) {
	nodeID := a.searchSourceNodeID()
	if nodeID == 0 || a.searchSourceRoster == nil {
		return 0, nil, errSearchSourceRoster
	}
	ids, err := a.searchSourceRoster()
	if err != nil || len(ids) != 1 || ids[0] != nodeID {
		return 0, nil, errors.Join(errSearchSourceRoster, err)
	}
	return nodeID, []uint64{nodeID}, nil
}

func (a *rpc) searchSourceTail(channelID string, channelType uint8) (uint64, uint64, error) {
	applied, err := a.searchSourceStore.GetAppliedMsgSeq(channelID, channelType)
	if err != nil {
		return 0, 0, err
	}
	physical, err := a.searchSourceStore.GetLastMsgSeq(channelID, channelType)
	if err != nil {
		return 0, 0, err
	}
	if applied > wkdb.MaxMessageSequence || physical > wkdb.MaxMessageSequence || applied > physical {
		return 0, 0, fmt.Errorf("invalid search source watermarks: applied=%d physical=%d", applied, physical)
	}
	return applied, physical, nil
}

func (a *rpc) readStableSearchSourceChannel(candidate wkdb.ChannelClusterConfig, nodeID uint64) (searchSourceChannel, error) {
	configID, channelID, channelType := candidate.Id, candidate.ChannelId, candidate.ChannelType
	for attempt := 0; attempt < searchSourceChannelReadAttempts; attempt++ {
		applied, physical, err := a.searchSourceTail(channelID, channelType)
		if err != nil {
			return searchSourceChannel{}, err
		}
		current, err := a.searchSourceAuthority(channelID, channelType)
		if err != nil {
			return searchSourceChannel{}, err
		}
		if !validSearchSourceSingleNodeConfig(current, nodeID) || current.Id != configID ||
			current.ChannelId != channelID || current.ChannelType != channelType {
			return searchSourceChannel{}, errSearchSourceFence
		}
		if candidate.Equal(current) {
			return searchSourceChannel{
				ConfigID: current.Id, ChannelID: current.ChannelId, ChannelType: current.ChannelType,
				LeaderID: current.LeaderId, Term: current.Term, ConfigVersion: current.ConfVersion,
				LastMessageSeq: applied, AppliedMessageSeq: applied, PhysicalMessageSeq: physical,
				ApplyPending: physical > applied, OfflineBootstrapRequired: applied == 0 && physical > 0,
			}, nil
		}
		candidate = current
	}
	return searchSourceChannel{}, errSearchSourceFence
}

func validSearchSourceSingleNodeConfig(cfg wkdb.ChannelClusterConfig, nodeID uint64) bool {
	return cfg.Id > 0 && strings.TrimSpace(cfg.ChannelId) != "" && cfg.ChannelType > 0 &&
		cfg.Status == wkdb.ChannelClusterStatusNormal && cfg.LeaderId == nodeID &&
		len(cfg.Replicas) == 1 && cfg.Replicas[0] == nodeID && len(cfg.Learners) == 0 &&
		cfg.MigrateFrom == 0 && cfg.MigrateTo == 0 && cfg.Term > 0 && cfg.ConfVersion > 0
}

func searchSourceOwns(cfg wkdb.ChannelClusterConfig, nodeID uint64, req searchSourceMessageRequest) bool {
	return validSearchSourceSingleNodeConfig(cfg, nodeID) && cfg.ChannelId == req.ChannelID &&
		cfg.ChannelType == req.ChannelType && cfg.LeaderId == req.ExpectedLeaderID &&
		cfg.Term == req.ExpectedTerm && cfg.ConfVersion == req.ExpectedConfigVersion
}

func searchSourceNotOwner(resp searchSourceMessageResponse, cfg wkdb.ChannelClusterConfig) searchSourceMessageResponse {
	resp.LeaderID, resp.Term, resp.ConfigVersion = cfg.LeaderId, cfg.Term, cfg.ConfVersion
	resp.NotOwner, resp.Retryable = true, true
	resp.LastMessageSeq, resp.AppliedMessageSeq, resp.PhysicalMessageSeq = 0, 0, 0
	resp.ApplyPending, resp.OfflineBootstrapRequired, resp.CaughtUp = false, false, false
	resp.ErrorKind, resp.ErrorCode = "", ""
	resp.Messages = make([]searchSourceMessage, 0)
	return resp
}

func searchSourceChannelLimit(limit int) (int, error) {
	if limit < 0 {
		return 0, errSearchSourceLimit
	}
	if limit == 0 {
		return searchSourceDefaultChannelLimit, nil
	}
	if limit > searchSourceMaxChannelLimit {
		return searchSourceMaxChannelLimit, nil
	}
	return limit, nil
}

func validateSearchSourceMessageRequest(req searchSourceMessageRequest) error {
	if req.Version != searchSourceProtocolVersion {
		return errSearchSourceVersion
	}
	if strings.TrimSpace(req.ChannelID) == "" || len(req.ChannelID) > searchSourceMaxChannelIDBytes {
		return errSearchSourceChannelID
	}
	if req.ChannelType < wkproto.ChannelTypePerson || req.ChannelType > wkproto.ChannelTypeAgentGroup {
		return errSearchSourceChannelType
	}
	if req.Limit <= 0 || req.Limit > searchSourceMaxMessageLimit {
		return errSearchSourceLimit
	}
	if req.ExpectedLeaderID == 0 || req.ExpectedTerm == 0 || req.ExpectedConfigVersion == 0 {
		return errSearchSourceGeneration
	}
	if req.NextSeq > searchSourceMaxNextSeq {
		return errSearchSourceSequence
	}
	return nil
}

func validateSearchSourceStoredPage(req searchSourceMessageRequest, applied uint64, rows []wkdb.SearchSourceMessage) error {
	if len(rows) == 0 {
		return errors.New("search source snapshot is missing an applied message")
	}
	expected := req.NextSeq
	for _, row := range rows {
		message := row.Message
		if message.Framer.NoPersist {
			return errors.New("search source snapshot contains a no-persist message")
		}
		if uint64(message.MessageSeq) != expected || uint64(message.MessageSeq) > applied || message.ChannelType != req.ChannelType {
			return errors.New("search source snapshot page is not contiguous or belongs to another channel")
		}
		expected++
	}
	return nil
}

func searchSourceMessageFromDB(row wkdb.SearchSourceMessage) searchSourceMessage {
	message := row.Message
	payload := message.Payload
	if row.PayloadOmitted {
		payload = nil
	}
	return searchSourceMessage{
		MessageID: message.MessageID, MessageSeq: uint64(message.MessageSeq), ClientMsgNo: message.ClientMsgNo,
		StreamNo: message.StreamNo, Setting: message.Setting.Uint8(), Timestamp: message.Timestamp,
		Expire: message.Expire, FromUID: message.FromUID, ChannelID: message.ChannelID,
		ChannelType: message.ChannelType, Topic: message.Topic, Payload: payload,
		NoPersist: message.Framer.NoPersist, PayloadOmitted: row.PayloadOmitted, StorageTerm: message.Term,
	}
}

func boundSearchSourceMessageResponse(resp *searchSourceMessageResponse) error {
	maxBytes := searchSourceMessageResponseBytes - searchSourceResponseReserveBytes
	for {
		data, err := json.Marshal(resp)
		if err != nil {
			return err
		}
		if len(data) <= maxBytes {
			return nil
		}
		omitted := false
		for index := len(resp.Messages) - 1; index >= 0; index-- {
			if !resp.Messages[index].PayloadOmitted && len(resp.Messages[index].Payload) > 0 {
				resp.Messages[index].Payload = nil
				resp.Messages[index].PayloadOmitted = true
				omitted = true
				break
			}
		}
		if !omitted {
			return wkdb.ErrSearchSourceMessageResponseBudget
		}
	}
}
