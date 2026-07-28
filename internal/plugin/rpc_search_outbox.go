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
	searchOutboxProtocolVersion = 1
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
	AckSearchOutbox(
		identities []wkdb.SearchOutboxIdentity,
	) error
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

func (liveSearchOutboxStore) AckSearchOutbox(
	identities []wkdb.SearchOutboxIdentity,
) error {
	if service.Store == nil {
		return errSearchOutboxStore
	}
	return service.Store.DB().AckSearchOutbox(identities)
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
	Message           searchOutboxMessage       `json:"message"`
	AppliedMessageSeq uint64                    `json:"applied_message_seq"`
}

type searchOutboxPullResponse struct {
	Version         int                     `json:"version"`
	NodeID          uint64                  `json:"node_id"`
	Pending         uint64                  `json:"pending"`
	OldestCreatedAt int64                   `json:"oldest_created_at"`
	AppliedBlocked  uint64                  `json:"applied_blocked"`
	Records         []searchOutboxRPCRecord `json:"records"`
}

type searchOutboxAckRequest struct {
	Version    int                         `json:"version"`
	NodeID     uint64                      `json:"node_id"`
	Identities []wkdb.SearchOutboxIdentity `json:"identities"`
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
	if len(req.Identities) == 0 ||
		len(req.Identities) > searchOutboxMaxAckCount {
		return searchOutboxAckResponse{}, errSearchOutboxAckCount
	}
	seen := make(map[wkdb.SearchOutboxIdentity]struct{}, len(req.Identities))
	for _, identity := range req.Identities {
		if err := identity.Validate(); err != nil {
			return searchOutboxAckResponse{}, err
		}
		if _, ok := seen[identity]; ok {
			return searchOutboxAckResponse{}, errSearchOutboxDuplicateIdentity
		}
		seen[identity] = struct{}{}
	}
	if a.searchOutboxStore == nil {
		return searchOutboxAckResponse{}, errSearchOutboxStore
	}
	if err := a.searchOutboxStore.AckSearchOutbox(req.Identities); err != nil {
		return searchOutboxAckResponse{}, err
	}
	return searchOutboxAckResponse{
		Version:      searchOutboxProtocolVersion,
		NodeID:       nodeID,
		Acknowledged: len(req.Identities),
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
