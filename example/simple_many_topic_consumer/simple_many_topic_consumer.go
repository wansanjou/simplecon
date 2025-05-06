package simpleconsumer

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
	c, err := simplecon.NewConsumer[Message]("localhost", []string{"first_topic", "second_topic"}, "group_id", &service{})
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

func (s *service) Send(ctx context.Context, in *simplecon.Message[Message]) (simplecon.Response, error) {
	switch in.Topic {
	case "first_topic":
		// do something
	case "second_topic":
		// do something
	}
	// do something
	return nil, nil
}
