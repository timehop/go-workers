package workers

import (
	"os"
	"testing"
	"time"

	"github.com/customerio/gospec"
)

func TestMain(m *testing.M) {
	Configure(map[string]string{
		"server":  os.Getenv("REDIS_URL"), // default from environment
		"process": "test",
	})

	// Optional: Wait for Redis to be ready
	for i := 0; i < 5; i++ {
		conn := Config.Pool.Get()
		_, err := conn.Do("PING")
		conn.Close()
		if err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}

	os.Exit(m.Run())
}

// You will need to list every spec in a TestXxx method like this,
// so that gotest can be used to run the specs. Later GoSpec might
// get its own command line tool similar to gotest, but for now this
// is the way to go. This shouldn't require too much typing, because
// there will be typically only one top-level spec per class/feature.

func TestAllSpecs(t *testing.T) {
	r := gospec.NewRunner()

	r.Parallel = false

	r.BeforeEach = func() {
		server := os.Getenv("REDIS_URL")
		if server == "" {
			server = "localhost:6379"
		}
		Configure(map[string]string{
			"server":   server,
			"process":  "1",
			"database": "15",
			"pool":     "1",
		})

		conn := Config.Pool.Get()
		conn.Do("flushdb")
		conn.Close()
	}

	// List all specs here
	r.AddSpec(WorkersSpec)
	r.AddSpec(ConfigSpec)
	r.AddSpec(MsgSpec)
	r.AddSpec(FetchSpec)
	r.AddSpec(WorkerSpec)
	r.AddSpec(ManagerSpec)
	r.AddSpec(ScheduledSpec)
	r.AddSpec(EnqueueSpec)
	r.AddSpec(QueueSpec)
	r.AddSpec(MiddlewareSpec)
	r.AddSpec(MiddlewareRetrySpec)
	r.AddSpec(MiddlewareStatsSpec)

	// Run GoSpec and report any errors to gotest's `testing.T` instance
	gospec.MainGoTest(r, t)
}
