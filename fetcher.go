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
	ready    chan bool
	messages chan *Msg
	stop     chan struct{}
	exit     chan struct{}
	closed   atomic.Bool
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

	for i, message := range messages {
		select {
		case <-f.stop:
			return
		case <-f.Ready():
			msg, err := NewMsg(message)
			if err != nil {
				Logger.Println("ERR: Couldn't parse old message:", err)
				continue
			}

			if upgradeLegacyRetryFormatIfNeeded(msg) {
				conn := Config.Pool.Get()
				_, err := conn.Do("lset", f.inprogressQueue(), i, msg.ToJson())
				conn.Close()

				if err != nil {
					Logger.Println("ERR: Failed to write upgraded retry format to Redis:", err)
				} else {
					Logger.Println("Upgraded legacy retry format for job", msg.Jid())
				}
			}

			f.sendMessage(msg.ToJson())
		}
	}
}

// TODO: Technically, we can just re-implement the Fetcher in the memories repo with this upgraded retry logic to simplify things?..
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
	return fmt.Sprint(f.queue, ":", Config.processId, ":inprogress")
}

func upgradeLegacyRetryFormatIfNeeded(msg *Msg) (wasUpgraded bool) {
	if _, ok := msg.CheckGet("retry_enabled"); ok {
		// Already upgraded
		return false
	}

	retryEnabled := false
	max := DEFAULT_MAX_RETRY

	if param, err := msg.Get("retry").Bool(); err == nil {
		retryEnabled = param
	} else if param, err := msg.Get("retry").Int(); err == nil {
		retryEnabled = true
		max = param
	} else {
		// Couldn't find legacy 'retry', don't upgrade
		return false
	}

	// Upgrade to new format
	msg.Set("retry_enabled", retryEnabled)
	msg.Set("max_retries", max)
	msg.Del("retry")
	return true
}
