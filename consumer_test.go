package pubsub

import (
	"context"
	"fmt"
	"github.com/furdarius/rabbitroutine"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
	"log"
	"os"
	"testing"
)

type TestJob struct {
	Publisher rabbitroutine.Publisher
}

func (job TestJob) Process(payload []byte) error {

	msg := string(payload)
	fmt.Println(msg)

	if msg == "ping" {
		err := job.Publisher.Publish(context.Background(), "eventbus", "test", amqp.Publishing{
			Body:     []byte("pong"),
			Priority: 99,
		})
		if err != nil {
			log.Println(err)
		}
	}

	return nil
}

func TestNewConsumer(t *testing.T) {

	url := os.Getenv("RABBITMQ_URL")

	logger, _ := zap.NewDevelopment()
	logger.Info("Initialize rabbit connection")
	pubSub, amqpErrCn := New(url, logger)

	consumer := NewConsumer(
		"eventbus",
		"test",
		logger,
		TestJob{Publisher: pubSub.Publisher},
		WithMaxPriority(100),
		WithRoutingKey("test"),
		WithDurable(false),
		WithAutoDelete(true),
	)
	err := pubSub.Connector.StartMultipleConsumers(context.Background(), consumer, 2)
	if err != nil {
		t.Error(err)
	}

	select {
	case err := <-amqpErrCn:
		t.Error(err)
	}
}
