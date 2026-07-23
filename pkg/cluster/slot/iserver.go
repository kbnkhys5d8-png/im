package slot

import (
	"context"
	"fmt"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"go.uber.org/zap"
)

func (s *Server) GetSlotId(v string) uint32 {

	return s.getSlotId(v)
}

func (s *Server) SlotLeaderId(slotId uint32) uint64 {
	slotConfig := s.opts.Node.Slot(slotId)
	if slotConfig != nil {
		return slotConfig.Leader
	}
	return 0

}

func (s *Server) Propose(slotId uint32, data []byte) (*types.ProposeResp, error) {
	shardNo := SlotIdToKey(slotId)
	logId := s.GenLogId()
	return s.raftGroup.Propose(shardNo, logId, data)
}

func (s *Server) ProposeUntilApplied(slotId uint32, data []byte) (*types.ProposeResp, error) {

	start := time.Now()
	defer func() {
		if trace.GlobalTrace != nil {
			trace.GlobalTrace.Metrics.Cluster().ProposeLatencyAdd(trace.ClusterKindSlot, time.Since(start).Milliseconds())
			if err := recover(); err != nil {
				trace.GlobalTrace.Metrics.Cluster().ProposeFailedCountAdd(trace.ClusterKindSlot, 1)
			}
		}
	}()

	shardNo := SlotIdToKey(slotId)
	logId := s.GenLogId()
	return s.raftGroup.ProposeUntilApplied(shardNo, logId, data)
}

func (s *Server) ProposeUntilAppliedTimeout(ctx context.Context, slotId uint32, data []byte) (*types.ProposeResp, error) {

	start := time.Now()
	defer func() {
		if trace.GlobalTrace != nil {
			trace.GlobalTrace.Metrics.Cluster().ProposeLatencyAdd(trace.ClusterKindSlot, time.Since(start).Milliseconds())
			if err := recover(); err != nil {
				trace.GlobalTrace.Metrics.Cluster().ProposeFailedCountAdd(trace.ClusterKindSlot, 1)
			}
		}
	}()

	slotConfig := s.opts.Node.Slot(slotId)
	if slotConfig == nil {
		return nil, fmt.Errorf("slot[%d] config not found", slotId)
	}

	logId := s.GenLogId()

	// 如果当前节点不是槽的领导节点，则向槽的领导节点请求提案
	if slotConfig.Leader != s.opts.NodeId {

		resps, err := s.opts.RPC.RequestSlotProposeBatchUntilApplied(slotConfig.Leader, slotId, types.ProposeReqSet{
			{
				Id:   logId,
				Data: data,
			},
		})
		if err != nil {
			return nil, err
		}
		if len(resps) == 0 {
			return nil, nil
		}
		return resps[0], nil
	}

	// 如果当前节点是槽的领导节点，则本地提案
	resps, err := s.ProposeUntilAppliedTimeoutForLocal(ctx, slotId, types.ProposeReqSet{
		{
			Id:   logId,
			Data: data,
		},
	})
	if err != nil {
		return nil, err
	}
	if len(resps) == 0 {
		return nil, nil
	}
	return resps[0], nil
}

func (s *Server) ProposeUntilAppliedTimeoutForLocal(ctx context.Context, slotId uint32, reqs types.ProposeReqSet) (types.ProposeRespSet, error) {
	shardNo := SlotIdToKey(slotId)

	resps, err := s.raftGroup.ProposeBatchUntilAppliedTimeout(ctx, shardNo, reqs)
	if err != nil {
		return nil, err
	}
	if len(resps) == 0 {
		return nil, nil
	}
	return resps, nil
}

func (s *Server) MustWaitAllSlotsReady(timeout time.Duration) {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			slots := s.opts.Node.Slots()
			slotCount := s.opts.Node.SlotCount()
			if slotCount == 0 {
				slotCount = s.opts.SlotCount
			}
			if len(slots) != int(slotCount) {
				continue
			}

			ready := true
			for _, slotConfig := range slots {
				if slotConfig.Leader == 0 {
					ready = false
					break
				}
				isLocal := wkutil.ArrayContainsUint64(slotConfig.Replicas, s.opts.NodeId) ||
					wkutil.ArrayContainsUint64(slotConfig.Learners, s.opts.NodeId)
				if !isLocal {
					continue
				}
				slotRaft := s.raftGroup.GetRaft(SlotIdToKey(slotConfig.Id))
				if slotRaft == nil || slotRaft.LeaderId() == 0 {
					ready = false
					break
				}
			}
			if ready {
				return
			}
		case <-timeoutCtx.Done():
			s.Panic("wait all slots ready timeout", zap.Error(timeoutCtx.Err()))
			return
		}
	}
}
