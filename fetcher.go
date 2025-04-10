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
	queue    string
	processID  string
	ready    chan bool
	messages chan *Msg
	stop     chan struct{}
	exit     chan struct{}
	closed   atomic.Bool
}

func NewFetch(queue string, processID string, messages chan *Msg, ready chan bool) Fetcher {

	return &fetch{
		queue:    queue,
		processID: processID,
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

func (f *fetch) sendMessage(message string) {
	msg, err := NewMsg(message)

	if err != nil {
		Logger.Println("ERR: Couldn't create message from", message, ":", err)
		return
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
	return fmt.Sprint(f.queue, ":", f.processID, ":inprogress")
}
