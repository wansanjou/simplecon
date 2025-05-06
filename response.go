package simplecon

type Response interface {
	Topic() string
	Data() interface{}
	Key() string
}

type response struct {
	key   string
	topic string
	data  interface{}
}

func NewResponse(key, topic string, data interface{}) Response {
	return &response{key, topic, data}
}

func (r *response) Topic() string {
	return r.topic
}

func (r *response) Key() string {
	return r.key
}

func (r *response) Data() interface{} {
	return r.data
}
