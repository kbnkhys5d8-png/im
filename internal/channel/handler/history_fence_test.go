package handler

import (
	"strings"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

type historyNoPersistChannel struct{ events []*eventbus.Event }

func (c *historyNoPersistChannel) AddEvent(_ string, _ uint8, e *eventbus.Event) {
	c.events = append(c.events, e)
}
func (c *historyNoPersistChannel) Advance(string, uint8) {}

func TestHistoryNoPersistFinalDistributionGate(t *testing.T) {
	beforeOptions, beforeStore, beforeChannel := options.G, service.Store, eventbus.Channel
	options.G = options.New()
	defer func() { options.G, service.Store, eventbus.Channel = beforeOptions, beforeStore, beforeChannel }()
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1)))
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err := db.(wkdb.HistoryFenceStore).FreezeHistoryFence(wkdb.HistoryFence{TaskID: strings.Repeat("a", 64), SourceUID: "a", PeerUID: "c", ChannelID: "a@c"})
	if err != nil {
		t.Fatal(err)
	}
	service.Store = store.New(store.NewOptions(store.WithDB(db), store.WithHistorySourceChannel(func(ch string) string { return strings.TrimSuffix(ch, "&cmd") })))
	for _, ch := range []string{"a@c", "a@c&cmd", "b@c"} {
		capture := &historyNoPersistChannel{}
		eventbus.RegisterChannel(capture)
		e := &eventbus.Event{Frame: &wkproto.SendPacket{Framer: wkproto.Framer{NoPersist: true}}, Conn: &eventbus.Conn{Uid: "a"}, ReasonCode: wkproto.ReasonSuccess}
		NewHandler().persist(&eventbus.ChannelContext{ChannelId: ch, ChannelType: 1, Events: []*eventbus.Event{e}})
		if ch == "b@c" {
			if len(capture.events) != 1 {
				t.Fatal("普通 NoPersist 被误封")
			}
		} else if len(capture.events) != 0 || e.ReasonCode == wkproto.ReasonSuccess {
			t.Fatal("NoPersist 越过冻结门禁", ch)
		}
	}
}
