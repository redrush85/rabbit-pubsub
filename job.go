package pubsub

type Job interface {
	Process(payload []byte) error
}
