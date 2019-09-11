package pubsub

import (
	"context"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
	"runtime/debug"
)

type Consumer struct {
	ExchangeName string
	QueueName    string
	logger       *zap.Logger
	Job          Job
	quit         chan bool
	config       *queueConfig
}

func NewConsumer(exchange, queue string, logger *zap.Logger, job Job, options ...QueueOption) *Consumer {
	cfg := getDefaultQueueOptions()
	for _, opt := range options {
		opt(cfg)
	}

	return &Consumer{
		ExchangeName: exchange,
		QueueName:    queue,
		logger:       logger,
		Job:          job,
		quit:         make(chan bool),
		config:       cfg,
	}
}

func (c Consumer) Declare(ctx context.Context, ch *amqp.Channel) error {
	err := ch.ExchangeDeclare(
		c.ExchangeName, // name
		"topic",        // type
		false,          // durable
		false,          // auto-deleted
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		c.logger.Error("failed to declare exchange", zap.String("exchange", c.ExchangeName), zap.Error(err))
		return err
	}

	_, err = ch.QueueDeclare(
		c.QueueName,         // name
		c.config.Durable,    // durable
		c.config.AutoDelete, // delete when usused
		c.config.Exclusive,  // exclusive
		c.config.NoWait,     // no-wait
		c.config.Args,       // arguments
	)
	if err != nil {
		c.logger.Error("failed to declare queue", zap.String("queue", c.QueueName), zap.Error(err))
		return err
	}

	err = ch.QueueBind(
		c.QueueName,         // queue name
		c.config.RoutingKey, // routing key
		c.ExchangeName,      // exchange
		c.config.NoWait,     // no-wait
		c.config.Args,       // arguments
	)
	if err != nil {
		c.logger.Error(
			"failed to bind queue",
			zap.String("queue", c.QueueName),
			zap.String("routing_key", c.config.RoutingKey),
			zap.Error(err),
		)
		return err
	}

	return nil

}

func (c Consumer) Consume(ctx context.Context, ch *amqp.Channel) error {
	defer c.logger.Info("consume method finished")

	err := ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		c.logger.Error("failed to set qos", zap.Error(err))
		return err
	}

	msgs, err := ch.Consume(
		c.QueueName, // queue
		"",          // consumer name
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		c.logger.Error("failed to consume", zap.String("queue", c.QueueName), zap.Error(err))
		return err
	}

	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				return amqp.ErrClosed
			}

			func() {
				defer func() {
					if r := recover(); r != nil {
						c.logger.Warn(
							"recovered from panic",
							zap.Any("reason", r),
							zap.String("stack", string(debug.Stack())),
						)
					}

					err = msg.Ack(false)
					if err != nil {
						c.logger.Error("failed to Ack message", zap.Error(err))
					}
				}()

				err = c.Job.Process(msg.Body)
				if err != nil {
					c.logger.Error("failed to process message", zap.Error(err))
				}
			}()

		case <-ctx.Done():
			return ctx.Err()
		case <-c.quit:
			c.logger.Info("Consumer stopped")
			return nil
		}

	}
}

// Stop worker
func (c *Consumer) Stop() {
	c.quit <- true
}
