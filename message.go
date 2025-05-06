package simplecon

type DLQMessage struct {
	Topic       string
	ClientId    string
	GroupId     string
	Message     string
	Description string
}

type Message[T any] struct {
	Data  *T
	Topic string
}
