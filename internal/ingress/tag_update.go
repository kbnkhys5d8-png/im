package ingress

import "errors"

// ChannelTagManager contains the tag operations needed to update an active
// channel tag without coupling the helper to the global service manager.
type ChannelTagManager interface {
	UpdateChannelTag(channelID string, channelType uint8, tagKey string, uids []string, remove bool) error
}

func validateTagUpdateReq(req *TagUpdateReq) error {
	if req.TagKey == "" && req.ChannelId == "" {
		return errors.New("tagKey and channelId are empty")
	}
	if req.ChannelTag {
		if req.ChannelId == "" {
			return errors.New("channelId is empty for channel tag invalidation")
		}
		return nil
	}
	if len(req.Uids) == 0 {
		return errors.New("uids are empty")
	}
	return nil
}

// UpdateChannelTag updates an active channel tag. A failed or missing tag is
// invalidated so the next message rebuilds it from the durable subscribers.
func UpdateChannelTag(manager ChannelTagManager, channelID string, channelType uint8, uids []string, remove bool) error {
	return manager.UpdateChannelTag(channelID, channelType, "", uids, remove)
}

// UpdateChannelTagWithKey preserves compatibility with older RPC callers that
// sent the active tag key explicitly.
func UpdateChannelTagWithKey(manager ChannelTagManager, channelID string, channelType uint8, tagKey string, uids []string, remove bool) error {
	return manager.UpdateChannelTag(channelID, channelType, tagKey, uids, remove)
}
