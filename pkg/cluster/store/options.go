package store

import (
	"context"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

type Options struct {
	NodeId uint64 // 节点ID

	Slot icluster.Slot

	DB wkdb.DB

	Channel icluster.Channel

	IsCmdChannel         func(channel string) bool
	HistoryTopology      func(channel string, channelType uint8) error
	HistoryCapabilities  func() error
	HistoryDrain         func(context.Context, string, uint8) error
	HistorySourceChannel func(channel string) string
}

func WithHistoryDrain(drain func(context.Context, string, uint8) error) Option {
	return func(o *Options) { o.HistoryDrain = drain }
}

func WithHistoryTopology(check func(string, uint8) error) Option {
	return func(o *Options) { o.HistoryTopology = check }
}

func WithHistorySourceChannel(normalize func(string) string) Option {
	return func(o *Options) { o.HistorySourceChannel = normalize }
}

func WithHistoryCapabilities(check func() error) Option {
	return func(o *Options) { o.HistoryCapabilities = check }
}

func NewOptions(opt ...Option) *Options {
	opts := &Options{}
	for _, o := range opt {
		o(opts)
	}
	return opts
}

type Option func(*Options)

func WithNodeId(nodeId uint64) Option {
	return func(o *Options) {
		o.NodeId = nodeId
	}
}

func WithSlot(slot icluster.Slot) Option {
	return func(o *Options) {
		o.Slot = slot
	}
}

func WithChannel(channel icluster.Channel) Option {
	return func(o *Options) {
		o.Channel = channel
	}
}

func WithDB(db wkdb.DB) Option {
	return func(o *Options) {
		o.DB = db
	}
}

func WithIsCmdChannel(isCmdChannel func(channel string) bool) Option {
	return func(o *Options) {
		o.IsCmdChannel = isCmdChannel
	}
}
