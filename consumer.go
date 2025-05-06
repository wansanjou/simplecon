package simplecon

import (
	"context"
	"errors"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bytedance/sonic"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	_ "go.uber.org/automaxprocs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

type Service[T any] interface {
	Send(ctx context.Context, in *Message[T]) (Response, error)
}

type SimpleConsumer interface {
	Consume()
	Close() error
	StartHealthzServer(port string, interceptor grpc.UnaryServerInterceptor)
}

type consumer[T any] struct {
	c *kafka.Consumer
	p *kafka.Producer
	o *options
	s Service[T]
}

func NewConsumer[T any](servers string, topic []string, groupId string, svc Service[T], opts ...Option) (SimpleConsumer, error) {
	if servers == "" {
		return nil, ErrEmptyServers
	}

	if len(topic) == 0 {
		return nil, ErrEmptyTopic
	}

	if groupId == "" {
		return nil, ErrEmptyGroupId
	}

	o := &options{
		servers:            servers,
		topic:              topic,
		groupId:            groupId,
		clientId:           "simcon",
		sessionTimeoutMs:   45000,
		readMessageTimeout: 100 * time.Millisecond,
		offsetReset:        "earliest",
		result:             false,
		lingerMs:           10,
	}

	for _, opt := range opts {
		opt(o)
	}

	consumerOpts := kafka.ConfigMap{
		"bootstrap.servers":  o.servers,
		"client.id":          o.clientId,
		"group.id":           o.groupId,
		"session.timeout.ms": o.sessionTimeoutMs,
		"auto.offset.reset":  o.offsetReset,
		"enable.auto.commit": false,
	}

	if o.securityProtocol != "" {
		consumerOpts.SetKey("security.protocol", o.securityProtocol)
		consumerOpts.SetKey("sasl.mechanisms", o.saslMechanism)
		consumerOpts.SetKey("sasl.username", o.username)
		consumerOpts.SetKey("sasl.password", o.password)
	}

	c, err := kafka.NewConsumer(&consumerOpts)
	if err != nil {
		return nil, err
	}

	var p *kafka.Producer
	if o.result {
		p, err = kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers":        o.servers,
			"linger.ms":                o.lingerMs,
			"client.id":                o.clientId,
			"compression.type":         "lz4",
			"acks":                     -1,
			"enable.idempotence":       true,
			"allow.auto.create.topics": true,
			"security.protocol":        o.securityProtocol,
			"sasl.mechanisms":          o.saslMechanism,
			"sasl.username":            o.username,
			"sasl.password":            o.password,
		})
		if err != nil {
			return nil, err
		}
	}

	return &consumer[T]{c: c, o: o, s: svc, p: p}, nil
}

func (c *consumer[T]) Close() error {
	return c.c.Close()
}

func (c *consumer[T]) Consume() {
	// recover from panic
	defer func() {
		if r := recover(); r != nil {
			log.Error().Msgf("recovered from panic: %v", r)
		}
		if c.p != nil {
			log.Info().Msg("flushing producer")
			c.p.Flush(60 * 1000)
			log.Info().Msg("flushing done")
		}
	}()

	if err := c.c.SubscribeTopics(c.o.topic, nil); err != nil {
		panic(err)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

l:
	for {
		select {
		case sig := <-sigchan:
			log.Warn().Msgf("caught signal %v: terminating\n", sig)
			break l
		default:
			if err := c.readMessage(); err != nil {
				log.Error().Msg(err.Error())
				break l
			}
		}
	}
}

func (c *consumer[T]) readMessage() error {
	ev, err := c.c.ReadMessage(c.o.readMessageTimeout)
	if err != nil {
		return nil
	}

	start := time.Now()

	headers := MapHeaders(ev.Headers)
	if headers[CorrelationID.String()] == "" {
		headers[CorrelationID.String()] = uuid.Must(uuid.NewV7()).String()
	}

	defer func() {
		log.Info().Str(CorrelationID.String(), headers[CorrelationID.String()]).Str("topic", *ev.TopicPartition.Topic).Int32("partition", ev.TopicPartition.Partition).Dur("latency", time.Since(start)).Msg(BytesToString(ev.Value))
	}()
	ctx := context.Background()

	md := metadata.New(map[string]string{
		CorrelationID.String(): headers[CorrelationID.String()],
	})

	return c.processMessage(metadata.NewOutgoingContext(ctx, md), ev)
}

func (c *consumer[T]) processMessage(ctx context.Context, ev *kafka.Message) error {
	var msg T
	if err := sonic.ConfigFastest.Unmarshal(ev.Value, &msg); err != nil {
		if c.o.dlqTopic != "" {
			if err := c.produceToDLQ(ctx, ev, err); err != nil {
				return errors.New("produce to DLQ error: " + err.Error())
			}
			return nil
		}
		log.Error().Msg("unmarshal message error: " + err.Error())
		c.c.Commit()
		return nil
	}

	result, err := c.s.Send(ctx, &Message[T]{
		Data:  &msg,
		Topic: *ev.TopicPartition.Topic,
	})
	if err != nil {
		if c.o.dlqTopic != "" {
			if err := c.produceToDLQ(ctx, ev, err); err != nil {
				return errors.New("produce to DLQ error: " + err.Error())
			}
			return nil
		}
		return errors.New("send message error: " + err.Error())
	}

	if !c.o.result || result == nil || result.Topic() == "" {
		c.c.Commit()
		return nil
	}

	var key []byte
	if result.Key() != "" {
		key = StringToBytes(result.Key())
	}

	b, err := sonic.ConfigFastest.Marshal(result.Data())
	if err != nil {
		return err
	}

	topic := result.Topic()

	if err := c.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   key,
		Value: b,
	}, nil); err != nil {
		return errors.New("produce message error: " + err.Error())
	}

	c.c.Commit()
	return nil
}

func (c *consumer[T]) produceToDLQ(ctx context.Context, ev *kafka.Message, errMsg error) error {
	input, _ := sonic.ConfigFastest.MarshalToString(ev.Value)
	msg := DLQMessage{
		Topic:       *ev.TopicPartition.Topic,
		ClientId:    c.o.clientId,
		GroupId:     c.o.groupId,
		Message:     input,
		Description: errMsg.Error(),
	}

	b, err := sonic.ConfigFastest.Marshal(msg)
	if err != nil {
		return err
	}

	topic := c.o.dlqTopic

	if err := c.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   nil,
		Value: b,
	}, nil); err != nil {
		return errors.New("produce message error: " + err.Error())
	}

	c.c.Commit()

	return nil
}

func (c *consumer[T]) StartHealthzServer(p string, interceptor grpc.UnaryServerInterceptor) {
	addr := "0.0.0.0:" + p
	var opts []grpc.ServerOption
	if interceptor != nil {
		opts = append(opts, grpc.UnaryInterceptor(interceptor))
	}
	grpcServer := grpc.NewServer(opts...)

	hzhdl := NewGrpc()
	grpc_health_v1.RegisterHealthServer(grpcServer, hzhdl)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}

	go func() {
		PrintWelcomeTextGRPC()
		defer grpcServer.GracefulStop()
		if err := grpcServer.Serve(lis); err != nil {
			panic(err)
		}
	}()
}
