package channel

import "errors"

var (
	ErrNoAllowVoteNode = errors.New("no allow vote node")
	ErrNoLeader        = errors.New("no leader")
	errStaleConfigTerm = errors.New("channel config term is lower than current raft term")
)
