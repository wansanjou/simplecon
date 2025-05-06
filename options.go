package simplecon

import (
	"time"
)

type options struct {
	servers            string
	username           string
	password           string
	groupId            string
	clientId           string
	sessionTimeoutMs   int
	readMessageTimeout time.Duration
	topic              []string
	dlqTopic           string
	securityProtocol   string
	saslMechanism      string
	offsetReset        string
	result             bool
	lingerMs           int
}

type Option func(*options)

func WithAuthSASLPlain(username, password string) Option {
	return func(o *options) {
		o.saslMechanism = "PLAIN"
		o.securityProtocol = "SASL_SSL"
		o.username = username
		o.password = password
	}
}

func WithSessionTimeoutMs(sessionTimeoutMs int) Option {
	return func(o *options) {
		o.sessionTimeoutMs = sessionTimeoutMs
	}
}

func WithClientId(clientId string) Option {
	return func(o *options) {
		o.clientId = clientId
	}
}

func WithReadMessageTimeout(readMessageTimeout time.Duration) Option {
	return func(o *options) {
		o.readMessageTimeout = readMessageTimeout
	}
}

func WithResult(result bool) Option {
	return func(o *options) {
		o.result = result
	}
}

func WithLingerMs(lingerMs int) Option {
	return func(o *options) {
		o.lingerMs = lingerMs
	}
}

func WithDLQTopic(dlqTopic string) Option {
	return func(o *options) {
		o.dlqTopic = dlqTopic
	}
}
