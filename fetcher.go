package workers

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/garyburd/redigo/redis"
)

type Fetcher interface {
	Queue() string
	Fetch()
	Acknowledge(*Msg)
	Ready() chan bool
	Messages() chan *Msg
	Close()
	Closed() bool
}

type fetch struct {
	queue     string
	processID string
	ready     chan bool
	messages  chan *Msg
	stop      chan struct{}
	exit      chan struct{}
	closed    atomic.Bool
}

func NewFetch(queue string, messages chan *Msg, ready chan bool) Fetcher {

	return &fetch{
		queue:    queue,
		ready:    ready,
		messages: messages,
		stop:     make(chan struct{}),
		exit:     make(chan struct{}),
	}
}

func (f *fetch) Queue() string {
	return f.queue
}

func (f *fetch) processOldMessages() {
	messages := f.inprogressMessages()

	for _, message := range messages {
		select {
		case <-f.stop:
			return
		case <-f.Ready():
			f.sendMessage(message)
		}
	}
}

func (f *fetch) Fetch() {
	defer close(f.exit)

	f.processOldMessages()

	for {
		select {
		case <-f.stop:
			f.closed.Store(true)
			return
		case <-f.Ready():
			conn := Config.Pool.Get()
			message, err := redis.String(conn.Do("brpoplpush", f.queue, f.inprogressQueue(), 1))
			conn.Close()

			if err != nil {
				if err.Error() != "redigo: nil returned" {
					Logger.Println("ERR: ", err)
					time.Sleep(1 * time.Second)
				}
				continue
			}

			f.sendMessage(message)
		}
	}
}

func (f *fetch) sendMessage(raw string) {
	msg, err := NewMsg(raw)
	if err != nil {
		Logger.Println("ERR: Couldn't create message from", raw, ":", err)
		return
	}

	// Handle legacy format upgrade
	if _, ok := msg.CheckGet("retry_enabled"); !ok {
		oldRaw := msg.OriginalJson()
		_ = retry(msg) // hack to convert

		// Put updated message back onto queue
		conn := Config.Pool.Get()
		defer conn.Close()

		// Find index of the original message
		index := -1
		items, _ := redis.Strings(conn.Do("lrange", f.inprogressQueue(), 0, -1))
		for i, item := range items {
			if item == oldRaw {
				index = i
				break
			}
		}

		if index != -1 {
			_, err := conn.Do("lset", f.inprogressQueue(), index, msg.ToJson())
			if err != nil {
				Logger.Println("ERR: Could not update legacy message in Redis:", err)
			} else {
				Logger.Println("Upgraded legacy retry format for job", msg.Jid())
			}
		} else {
			Logger.Println("WARN: Could not find original message in queue for", msg.Jid())
		}
	}

	f.Messages() <- msg
}

func (f *fetch) Acknowledge(message *Msg) {
	conn := Config.Pool.Get()
	defer conn.Close()
	conn.Do("lrem", f.inprogressQueue(), -1, message.OriginalJson())
}

func (f *fetch) Messages() chan *Msg {
	return f.messages
}

func (f *fetch) Ready() chan bool {
	return f.ready
}

func (f *fetch) Close() {
	select {
	case <-f.stop:
		// Nothing to do: already closed
	default:
		close(f.stop) // Safe if this gets called multiple times
	}
}

func (f *fetch) Closed() bool {
	return f.closed.Load()
}

func (f *fetch) inprogressMessages() []string {
	conn := Config.Pool.Get()
	defer conn.Close()

	messages, err := redis.Strings(conn.Do("lrange", f.inprogressQueue(), 0, -1))
	if err != nil {
		Logger.Println("ERR: ", err)
	}

	return messages
}

func (f *fetch) inprogressQueue() string {
	return fmt.Sprint(f.queue, ":", Config.processId, ":inprogress")
}
