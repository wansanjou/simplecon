package producer

import (
	"context"

	"github.com/rs/zerolog/log"

	"simplecon"
)

type Message struct {
	ID string `json:"id"`
}

func main() {
	// create new consumer, read message and unmarshal to Message struct
	c, err := simplecon.NewConsumer[Message]("localhost", []string{"topic"}, "group_id", &service{}, simplecon.WithResult(true))
	if err != nil {
		panic(err)
	}

	c.Consume()

	if err := c.Close(); err != nil {
		// bla bla
	}

	log.Error().Msg("exit")
}

// implement service (Send method)
type service struct{}

type Response struct{}

func (s *service) Send(ctx context.Context, in *simplecon.Message[Message]) (simplecon.Response, error) {
	// do something

	return simplecon.NewResponse("msg_key", "next_topic", &Response{}), nil
}
