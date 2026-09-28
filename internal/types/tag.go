package types

import (
	"fmt"
	"time"

	"go.uber.org/atomic"
)

type Tag struct {
	Key string `json:"key"`
	// 关联的频道
	ChannelId   string  `json:"channel_id"`
	ChannelType uint8   `json:"channel_type"`
	Nodes       []*Node `json:"nodes"`
	// 创建时间
	CreatedAt time.Time `json:"created_at"`
	// 最后一次获取时间，消息读取、过期清理及管理接口须原子读写。
	LastGetTime tagAccessTime `json:"last_get_time"`
	NodeVersion uint64        `json:"node_version"` // 生成tag时的当前节点版本号，如果当前节点版本号大于生成tag时的节点版本号，则tag失效
	GetCount    atomic.Uint64 `json:"get_count"`    // 获取次数
}

// tagAccessTime 保留 time.Time 的 JSON 格式及单调时钟信息，不暴露原子容器的内部表示。
type tagAccessTime struct {
	atomic.Time
}

func (t *tagAccessTime) MarshalJSON() ([]byte, error) {
	return t.Load().MarshalJSON()
}

func (t *tagAccessTime) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	value := t.Load()
	err := value.UnmarshalJSON(data)
	t.Store(value)
	return err
}

func (t *Tag) String() string {

	return fmt.Sprintf("Tag{Key:%s, Nodes:%v, LastGetTime:%v}", t.Key, t.Nodes, t.LastGetTime.Load())
}

type Node struct {
	// 节点id
	LeaderId uint64 `json:"leader_id"`
	// 节点id对应的用户集合
	Uids []string `json:"uids"`
	// 用户涉及到的slot
	SlotIds []uint32 `json:"slot_ids"`
}

func (n *Node) String() string {
	return fmt.Sprintf("Node{LeaderId:%d, Uids:%v, SlotIds:%v}", n.LeaderId, n.Uids, n.SlotIds)
}

func (t *Tag) GetNodeUsers(nodeId uint64) []string {
	for _, node := range t.Nodes {
		if node.LeaderId == nodeId {
			return node.Uids
		}
	}
	return nil
}

func (t *Tag) ExistUserInNode(uid string, nodeId uint64) bool {
	for _, node := range t.Nodes {
		if node.LeaderId == nodeId {
			for _, u := range node.Uids {
				if u == uid {
					return true
				}
			}
		}
	}
	return false
}

func (t *Tag) GetUsers() []string {
	userCount := 0
	for _, node := range t.Nodes {
		userCount += len(node.Uids)
	}
	users := make([]string, 0, userCount)
	for _, node := range t.Nodes {
		users = append(users, node.Uids...)
	}
	return users
}
