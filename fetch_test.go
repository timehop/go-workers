package workers

import (
	"time"

	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func buildFetch(queue string) Fetcher {
	manager := newManager(queue, nil, 1)
	fetch := manager.fetch
	go fetch.Fetch()
	return fetch
}

func FetchSpec(c Context) {
	c.Specify("Config.Fetch", func() {
		c.Specify("it returns an instance of fetch with queue", func() {
			f := buildFetch("fetchQueue1")
			defer func() {
				f.Close()
				<-f.(*fetch).exit
			}()
			c.Expect(f.Queue(), Equals, "queue:fetchQueue1")
		})
	})

	c.Specify("Fetch", func() {
		message, _ := NewMsg("{\"foo\":\"bar\"}")

		c.Specify("it puts messages from the queues on the messages channel", func() {
			f := buildFetch("fetchQueue2")
			// Close once we're done
			defer func() {
				f.Close()
				<-f.(*fetch).exit
			}()

			conn := Config.Pool.Get()
			defer conn.Close()

			conn.Do("lpush", "queue:fetchQueue2", message.ToJson())

			f.Ready() <- true

			select {
			case received := <-f.Messages():
				c.Expect(received.OriginalJson(), Equals, message.ToJson())
			case <-time.After(2 * time.Second):
				c.Expect("timed out waiting for message", Equals, "") // forces a failure
			}

			len, _ := redis.Int(conn.Do("llen", "queue:fetchQueue2"))
			c.Expect(len, Equals, 0)
		})

		c.Specify("places in progress messages on private queue", func() {
			f := buildFetch("fetchQueue3")
			// Close once we're done
			defer func() {
				f.Close()
				<-f.(*fetch).exit
			}()

			conn := Config.Pool.Get()
			defer conn.Close()

			conn.Do("lpush", "queue:fetchQueue3", message.ToJson())

			f.Ready() <- true
			<-f.Messages()

			len, _ := redis.Int(conn.Do("llen", "queue:fetchQueue3:1:inprogress"))
			c.Expect(len, Equals, 1)

			messages, _ := redis.Strings(conn.Do("lrange", "queue:fetchQueue3:1:inprogress", 0, -1))
			c.Expect(messages[0], Equals, message.ToJson())
		})

		c.Specify("removes in progress message when acknowledged", func() {
			f := buildFetch("fetchQueue4")
			// Close once we're done
			defer func() {
				f.Close()
				<-f.(*fetch).exit
			}()

			conn := Config.Pool.Get()
			defer conn.Close()

			conn.Do("lpush", "queue:fetchQueue4", message.ToJson())

			f.Ready() <- true
			<-f.Messages()

			f.Acknowledge(message)

			len, _ := redis.Int(conn.Do("llen", "queue:fetchQueue4:1:inprogress"))
			c.Expect(len, Equals, 0)
		})

		c.Specify("removes in progress message when serialized differently", func() {
			json := "{\"foo\":\"bar\",\"args\":[]}"
			message, _ := NewMsg(json)

			c.Expect(json, Not(Equals), message.ToJson())

			f := buildFetch("fetchQueue5")
			// Close once we're done
			defer func() {
				f.Close()
				<-f.(*fetch).exit
			}()

			conn := Config.Pool.Get()
			defer conn.Close()

			conn.Do("lpush", "queue:fetchQueue5", json)

			f.Ready() <- true
			<-f.Messages()

			f.Acknowledge(message)

			len, _ := redis.Int(conn.Do("llen", "queue:fetchQueue5:1:inprogress"))
			c.Expect(len, Equals, 0)
		})

		c.Specify("refires any messages left in progress from prior instance", func() {
			message2, _ := NewMsg("{\"foo\":\"bar2\"}")
			message3, _ := NewMsg("{\"foo\":\"bar3\"}")

			conn := Config.Pool.Get()
			defer conn.Close()

			// Create 2 message in progress
			conn.Do("lpush", "queue:fetchQueue6:1:inprogress", message.ToJson())
			conn.Do("lpush", "queue:fetchQueue6:1:inprogress", message2.ToJson())
			// Create a third, new message
			conn.Do("lpush", "queue:fetchQueue6", message3.ToJson())

			f := buildFetch("fetchQueue6")
			// Close once we're done
			defer func() {
				f.Close()
				<-f.(*fetch).exit
			}()

			f.Ready() <- true
			c.Expect(<-f.Messages(), Equals, message2)
			f.Ready() <- true
			c.Expect(<-f.Messages(), Equals, message)
			f.Ready() <- true
			c.Expect(<-f.Messages(), Equals, message3)

			f.Acknowledge(message)
			f.Acknowledge(message2)
			f.Acknowledge(message3)

			len, _ := redis.Int(conn.Do("llen", "queue:fetchQueue6:1:inprogress"))
			c.Expect(len, Equals, 0)
		})
	})
}
