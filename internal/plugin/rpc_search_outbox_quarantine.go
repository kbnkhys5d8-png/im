package plugin

import (
	"encoding/json"
	"errors"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/wkrpc"
)

type searchOutboxCapabilitiesRequest struct {
	Version int `json:"version"`
}

type searchOutboxCapabilitiesResponse struct {
	Version           int    `json:"version"`
	NodeID            uint64 `json:"node_id"`
	DurableQuarantine bool   `json:"durable_quarantine"`
	EpochFencing      bool   `json:"epoch_fencing"`
}

type searchOutboxTaskRequest struct {
	Version int                   `json:"version"`
	NodeID  uint64                `json:"node_id"`
	Task    wkdb.SearchOutboxTask `json:"task"`
	Reason  string                `json:"reason,omitempty"`
}

type searchOutboxQuarantineResponse struct {
	Version     int    `json:"version"`
	NodeID      uint64 `json:"node_id"`
	Quarantined bool   `json:"quarantined"`
}

type searchOutboxRestoreResponse struct {
	Version int    `json:"version"`
	NodeID  uint64 `json:"node_id"`
	Epoch   uint64 `json:"epoch"`
}

func (a *rpc) searchOutboxCapabilitiesRoute(c *wkrpc.Context) {
	var req searchOutboxCapabilitiesRequest
	if err := json.Unmarshal(c.Body(), &req); err != nil {
		c.WriteErr(errors.New("invalid search outbox capabilities request"))
		return
	}
	resp, err := a.searchOutboxCapabilities(req)
	if err != nil {
		c.WriteErr(err)
		return
	}
	a.writeSearchSourceJSON(c, resp)
}

func (a *rpc) searchOutboxCapabilities(req searchOutboxCapabilitiesRequest) (searchOutboxCapabilitiesResponse, error) {
	if err := a.requireSearchOutboxReady(); err != nil {
		return searchOutboxCapabilitiesResponse{}, err
	}
	if req.Version != searchOutboxProtocolVersion {
		return searchOutboxCapabilitiesResponse{}, errSearchOutboxVersion
	}
	nodeID, err := a.validSearchOutboxNodeID()
	if err != nil {
		return searchOutboxCapabilitiesResponse{}, err
	}
	if a.searchOutboxStore == nil {
		return searchOutboxCapabilitiesResponse{}, errSearchOutboxStore
	}
	return searchOutboxCapabilitiesResponse{
		Version: searchOutboxProtocolVersion, NodeID: nodeID,
		DurableQuarantine: true, EpochFencing: true,
	}, nil
}

func (a *rpc) validateSearchOutboxMutation(req searchOutboxTaskRequest) error {
	if err := a.requireSearchOutboxReady(); err != nil {
		return err
	}
	if req.Version != searchOutboxProtocolVersion {
		return errSearchOutboxVersion
	}
	nodeID, err := a.validSearchOutboxNodeID()
	if err != nil {
		return err
	}
	if req.NodeID == 0 || req.NodeID != nodeID {
		return errSearchOutboxNode
	}
	if err := validateSearchOutboxTask(req.Task); err != nil {
		return err
	}
	if a.searchOutboxStore == nil {
		return errSearchOutboxStore
	}
	return nil
}

func (a *rpc) searchOutboxQuarantineRoute(c *wkrpc.Context) {
	var req searchOutboxTaskRequest
	if err := json.Unmarshal(c.Body(), &req); err != nil {
		c.WriteErr(errors.New("invalid search outbox quarantine request"))
		return
	}
	resp, err := a.searchOutboxQuarantine(req)
	if err != nil {
		c.WriteErr(err)
		return
	}
	a.writeSearchSourceJSON(c, resp)
}

func (a *rpc) searchOutboxQuarantine(req searchOutboxTaskRequest) (searchOutboxQuarantineResponse, error) {
	if err := a.validateSearchOutboxMutation(req); err != nil {
		return searchOutboxQuarantineResponse{}, err
	}
	// 原文与底层错误不能经 RPC 写入原因；存储层还会复核固定枚举。
	if !wkdb.ValidSearchOutboxQuarantineReason(req.Reason) {
		return searchOutboxQuarantineResponse{}, errors.New("invalid search outbox quarantine reason")
	}
	if err := a.searchOutboxStore.QuarantineSearchOutbox(req.Task, req.Reason); err != nil {
		return searchOutboxQuarantineResponse{}, err
	}
	return searchOutboxQuarantineResponse{Version: searchOutboxProtocolVersion, NodeID: req.NodeID, Quarantined: true}, nil
}

func (a *rpc) searchOutboxRestoreRoute(c *wkrpc.Context) {
	var req searchOutboxTaskRequest
	if err := json.Unmarshal(c.Body(), &req); err != nil {
		c.WriteErr(errors.New("invalid search outbox restore request"))
		return
	}
	resp, err := a.searchOutboxRestore(req)
	if err != nil {
		c.WriteErr(err)
		return
	}
	a.writeSearchSourceJSON(c, resp)
}

func (a *rpc) searchOutboxRestore(req searchOutboxTaskRequest) (searchOutboxRestoreResponse, error) {
	if err := a.validateSearchOutboxMutation(req); err != nil {
		return searchOutboxRestoreResponse{}, err
	}
	// 仅显式恢复搜索任务，不发送聊天消息，也不直接写入 OpenSearch。
	epoch, err := a.searchOutboxStore.RestoreSearchOutbox(req.Task)
	if err != nil {
		return searchOutboxRestoreResponse{}, err
	}
	return searchOutboxRestoreResponse{Version: searchOutboxProtocolVersion, NodeID: req.NodeID, Epoch: epoch}, nil
}
