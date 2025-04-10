package workers

import (
	"sync"

	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

type customMid struct {
	sync.Mutex
	Trace []string
	Base  string
}

func (m *customMid) Call(queue string, message *Msg, next func() bool) (result bool) {
	m.Lock()
	m.Trace = append(m.Trace, m.Base+"1")
	m.Unlock()

	result = next()

	m.Lock()
	m.Trace = append(m.Trace, m.Base+"2")
	m.Unlock()
	return
}

func (m *customMid) TraceCopy() []string {
	m.Lock()
	defer m.Unlock()
	return append([]string{}, m.Trace...)
}

func ManagerSpec(c Context) {
	processed := make(chan *Args)

	testJob := (func(message *Msg) {
		processed <- message.Args()
	})

	was := Config.Namespace
	Config.Namespace = "prod:"

	c.Specify("newManager", func() {
		c.Specify("sets queue with namespace", func() {
			manager := newManager("myqueue", testJob, 10)
			c.Expect(manager.queue, Equals, "prod:queue:myqueue")
		})

		c.Specify("sets job function", func() {
			manager := newManager("myqueue", testJob, 10)
			c.Expect(manager.job != nil, IsTrue)
		})

		c.Specify("sets worker concurrency", func() {
			manager := newManager("myqueue", testJob, 10)
			c.Expect(manager.concurrency, Equals, 10)
		})

		c.Specify("no per-manager middleware means 'use global Middleware object'", func() {
			manager := newManager("myqueue", testJob, 10)
			c.Expect(manager.mids, Equals, Middleware)
		})

		c.Specify("per-manager middlewares create separate middleware chains", func() {
			mid1 := customMid{Trace: []string{}, Base: "0"}
			manager := newManager("myqueue", testJob, 10, &mid1)
			c.Expect(manager.mids, Not(Equals), Middleware)
			c.Expect(len(manager.mids.actions), Equals, len(Middleware.actions)+1)
		})

	})

	c.Specify("manage", func() {
		conn := Config.Pool.Get()
		defer conn.Close()

		message, _ := NewMsg("{\"foo\":\"bar\",\"args\":[\"foo\",\"bar\"]}")
		message2, _ := NewMsg("{\"foo\":\"bar2\",\"args\":[\"foo\",\"bar2\"]}")

		c.Specify("coordinates processing of queue messages", func() {
			manager := newManager("manager1", testJob, 10)

			conn.Do("lpush", "prod:queue:manager1", message.ToJson())
			conn.Do("lpush", "prod:queue:manager1", message2.ToJson())

			manager.start()

			c.Expect(<-processed, Equals, message.Args())
			c.Expect(<-processed, Equals, message2.Args())

			manager.quit()

			len, _ := redis.Int(conn.Do("llen", "prod:queue:manager1"))
			c.Expect(len, Equals, 0)
		})

		c.Specify("per-manager middlwares are called separately, global middleware is called in each manager", func() {
			mid1 := customMid{Trace: []string{}, Base: "1"}
			mid2 := customMid{Trace: []string{}, Base: "2"}
			mid3 := customMid{Trace: []string{}, Base: "3"}

			oldMiddleware := Middleware
			Middleware = NewMiddleware()
			Middleware.Append(&mid1)

			manager1 := newManager("manager1", testJob, 10)
			manager2 := newManager("manager2", testJob, 10, &mid2)
			manager3 := newManager("manager3", testJob, 10, &mid3)

			conn.Do("lpush", "prod:queue:manager1", message.ToJson())
			conn.Do("lpush", "prod:queue:manager2", message.ToJson())
			conn.Do("lpush", "prod:queue:manager3", message.ToJson())

			manager1.start()
			manager2.start()
			manager3.start()

			<-processed
			<-processed
			<-processed

			Middleware = oldMiddleware

			c.Expect(
				arrayCompare(mid1.TraceCopy(), []string{"11", "12", "11", "12", "11", "12"}),
				IsTrue,
			)
			c.Expect(
				arrayCompare(mid2.TraceCopy(), []string{"21", "22"}),
				IsTrue,
			)
			c.Expect(
				arrayCompare(mid3.TraceCopy(), []string{"31", "32"}),
				IsTrue,
			)

			manager1.quit()
			manager2.quit()
			manager3.quit()
		})

		c.Specify("prepare stops fetching new messages from queue", func() {
			manager := newManager("manager2", testJob, 10)
			manager.start()

			manager.prepare()

			conn.Do("lpush", "prod:queue:manager2", message)
			conn.Do("lpush", "prod:queue:manager2", message2)

			manager.quit()

			len, _ := redis.Int(conn.Do("llen", "prod:queue:manager2"))
			c.Expect(len, Equals, 2)
		})
	})

	Config.Namespace = was
}
