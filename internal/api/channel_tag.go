package api

import "github.com/WuKongIM/WuKongIM/internal/options"

type channelTagOwnerResolver interface {
	SlotLeaderIdOfChannel(channelID string, channelType uint8) (uint64, error)
}

// resolveChannelTagOwner returns the node that owns the complete subscriber
// tag used by message distribution.
func resolveChannelTagOwner(resolver channelTagOwnerResolver, channelID string, channelType uint8) (uint64, error) {
	return resolver.SlotLeaderIdOfChannel(channelID, channelType)
}

type channelTagInvalidationTarget struct {
	channelID   string
	channelType uint8
}

func channelTagInvalidationTargets(channelID string, channelType uint8, includeCMD bool) []channelTagInvalidationTarget {
	targets := []channelTagInvalidationTarget{{channelID: channelID, channelType: channelType}}
	if includeCMD {
		cmdChannelID := options.G.OrginalConvertCmdChannel(channelID)
		if cmdChannelID != channelID {
			targets = append(targets, channelTagInvalidationTarget{
				channelID:   cmdChannelID,
				channelType: channelType,
			})
		}
	}
	return targets
}

func failedChannelTagInvalidations(targets []channelTagInvalidationTarget, invalidate func(channelTagInvalidationTarget) error) []channelTagInvalidationTarget {
	var failures []channelTagInvalidationTarget
	for _, target := range targets {
		if err := invalidate(target); err != nil {
			failures = append(failures, target)
		}
	}
	return failures
}
