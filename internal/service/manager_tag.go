package service

import "github.com/WuKongIM/WuKongIM/internal/types"

var TagManager ITagManager

type ITagManager interface {
	// UpdateChannelTag invalidates a channel snapshot after its membership
	// changes. tagKey is optional and exists for old RPC callers.
	UpdateChannelTag(fakeChannelId string, channelType uint8, tagKey string, uids []string, remove bool) error
	// WithChannelTagLock serializes snapshot rebuilds and invalidations for the
	// same channel without a single global rebuild lock.
	WithChannelTagLock(fakeChannelId string, channelType uint8, fn func() error) error
	// AddUsers 向指定tag里增加用户
	AddUsers(tagKey string, uids []string) error
	RemoveUsers(tagKey string, uids []string) error
	// RemoveTag 移除tag
	RemoveTag(tagKey string)
	// RetireTag removes a tag from active use but keeps it briefly readable for
	// messages that were already stamped with that key.
	RetireTag(tagKey string)
	// RenameTag 重命名tag
	RenameTag(oldTagKey, newTagKey string) error
	// MakeTag 创建tag
	MakeTag(uids []string) (*types.Tag, error)
	// MakeTagWithTagKey 创建tag并指定tagKey
	MakeTagWithTagKey(tagKey string, uids []string) (*types.Tag, error)
	// MakeTagNotCacheWithTagKey 创建tag并指定tagKey，但是不缓存
	MakeTagNotCacheWithTagKey(tagKey string, uids []string) (*types.Tag, error)
	//	 GetUsers 获取tag下的所有用户
	GetUsers(tagKey string) []string
	// GetTag 获取tag
	Get(tagKey string) *types.Tag
	// Exist 是否存在tag
	Exist(tagKey string) bool
	// SetChannelTag 设置频道对应的tag
	SetChannelTag(fakeChannelId string, channelType uint8, tagKey string)
	// GetChannelTag 获取频道对应的tag
	GetChannelTag(fakeChannelId string, channelType uint8) string
	//	 RemoveChannelTag 移除频道对应的tag
	RemoveChannelTag(fakeChannelId string, channelType uint8)
	// GetAllTags 获取所有tag
	GetAllTags() []*types.Tag

	// GetAllChannelTags 获取所有频道对应的tag
	GetAllChannelTags() map[string]string
}
