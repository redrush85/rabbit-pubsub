package pubsub

import (
	"context"
	"github.com/furdarius/rabbitroutine"
	"go.uber.org/zap"
	"time"
)

type PubSub struct {
	Connector *rabbitroutine.Connector
	Publisher rabbitroutine.Publisher
}

func New(
	dialString string,
	logger *zap.Logger,
) (
	pubSub *PubSub,
	errCn chan error,
) {
	errCn = make(chan error)

	connector := rabbitroutine.NewConnector(rabbitroutine.Config{
		// Max reconnect attempts
		ReconnectAttempts: 20,
		// How long wait between reconnect
		Wait: 2 * time.Second,
	})

	publisher := rabbitroutine.NewRetryPublisher(
		rabbitroutine.NewEnsurePublisher(
			rabbitroutine.NewPool(
				connector,
			),
		),
		rabbitroutine.PublishMaxAttemptsSetup(10),
		rabbitroutine.PublishDelaySetup(rabbitroutine.LinearDelay(10*time.Millisecond)),
	)

	connector.AddRetriedListener(func(r rabbitroutine.Retried) {
		logger.Error("Try to connect to RabbitMQ",
			zap.Uint("attempt", r.ReconnectAttempt),
			zap.Error(r.Error),
		)
	})

	connector.AddDialedListener(func(_ rabbitroutine.Dialed) {
		logger.Info("RabbitMQ connection successfully established")
	})

	connector.AddAMQPNotifiedListener(func(n rabbitroutine.AMQPNotified) {
		logger.Error("RabbitMQ error received",
			zap.Error(n.Error),
		)
	})

	go func() {
		err := connector.Dial(context.TODO(), dialString)
		if err != nil {
			errCn <- err
			logger.Error(
				"RabbitMQ error dial",
				zap.Error(err),
			)
		}
	}()

	pubSub = &PubSub{
		Connector: connector,
		Publisher: publisher,
	}
	return
}
