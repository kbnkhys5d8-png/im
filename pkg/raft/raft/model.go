package raft

import "github.com/WuKongIM/WuKongIM/pkg/raft/types"

type stepReq struct {
	event types.Event
	resp  chan error
}

// 节点操作由事件循环执行，响应通道把操作结果同步给调用方。
type nodeActionReq struct {
	action func() error
	resp   chan error
}
