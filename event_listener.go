package pubsub

import "github.com/furdarius/rabbitroutine"

type EventListener struct {
	Publisher rabbitroutine.Publisher
}
