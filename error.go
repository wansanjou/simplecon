package simplecon

import "errors"

var (
	ErrEmptyServers = errors.New("servers is empty")
	ErrEmptyTopic   = errors.New("topic is empty")
	ErrEmptyGroupId = errors.New("groupId is empty")
)
