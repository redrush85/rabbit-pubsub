package pubsub

import "github.com/streadway/amqp"

type queueConfig struct {
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	BindNoWait bool
	RoutingKey string
	Args       amqp.Table
	BindArgs   amqp.Table
}

//QueueOption queue options
type QueueOption func(c *queueConfig)

func getDefaultQueueOptions() *queueConfig {
	return &queueConfig{
		Durable: true,
		Args:    amqp.Table{},
	}
}

//WithDurable set durable param for queue
func WithDurable(durable bool) QueueOption {
	return func(c *queueConfig) {
		c.Durable = durable
	}
}

//WithAutoDelete set autodelete param
func WithAutoDelete(autoDelete bool) QueueOption {
	return func(c *queueConfig) {
		c.AutoDelete = autoDelete
	}
}

//WithExclusive set exclusive
func WithExclusive(exclusive bool) QueueOption {
	return func(c *queueConfig) {
		c.Exclusive = exclusive
	}
}

//WithNoWait set no wait
func WithNoWait(noWait bool) QueueOption {
	return func(c *queueConfig) {
		c.NoWait = noWait
	}
}

//WithRoutingKey set routing key when bind queue to exchange
func WithRoutingKey(routingKey string) QueueOption {
	return func(c *queueConfig) {
		c.RoutingKey = routingKey
	}
}

//WithMaxPriority set max priority param for the queue
func WithMaxPriority(maxPriority uint8) QueueOption {
	return func(c *queueConfig) {
		c.Args["x-max-priority"] = maxPriority
	}
}
