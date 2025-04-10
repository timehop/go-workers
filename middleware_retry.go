package workers

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

const (
	DEFAULT_MAX_RETRY = 25
	LAYOUT            = "2006-01-02 15:04:05 MST"
)

type MiddlewareRetry struct{}

func (r *MiddlewareRetry) Call(queue string, message *Msg, next func() bool) (acknowledge bool) {
	defer func() {
		if e := recover(); e != nil {
			conn := Config.Pool.Get()
			defer conn.Close()

			if retry(message) {
				message.Set("queue", queue)
				message.Set("error_message", fmt.Sprintf("%v", e))
				retryCount := incrementRetry(message)

				waitDuration := durationToSecondsWithNanoPrecision(
					time.Duration(
						secondsToDelay(retryCount),
					) * time.Second,
				)

				_, err := conn.Do(
					"zadd",
					Config.Namespace+RETRY_KEY,
					nowToSecondsWithNanoPrecision()+waitDuration,
					message.ToJson(),
				)

				// If we can't add the job to the retry queue,
				// then we shouldn't acknowledge the job, otherwise
				// it'll disappear into the void.
				if err != nil {
					acknowledge = false
				}
			}

			panic(e)
		}
	}()

	acknowledge = next()

	return
}

func retry(message *Msg) bool {
	retryEnabled := false
	max := DEFAULT_MAX_RETRY

	// Attempt to use the new format
	if param, err := message.Get("retry_enabled").Bool(); err == nil {
		retryEnabled = param
	}
	if param, err := message.Get("max_retries").Int(); err == nil {
		max = param
	}

	// TODO: Add FF to eventually migrate fully to new retry format?
	// Backward compatibility: fall back to legacy "retry"
	if _, ok := message.CheckGet("retry_enabled"); !ok {
		if param, err := message.Get("retry").Bool(); err == nil {
			retryEnabled = param
		} else if param, err := message.Get("retry").Int(); err == nil {
			retryEnabled = true
			max = param
		}

		message.Set("retry_enabled", retryEnabled)
		message.Set("max_retries", max)
		message.Del("retry")

		// TODO: Log that we upgraded to the new retry format?
		// log.Info("Worker", "Upgraded legacy retry format", "jid", message.Jid(), "retry_enabled", retryEnabled, "max_retries", max)
		// TODO: Add stats?
		// stathat.Increment("memories.worker.job.retry.legacy_upgraded", 1)
	}

	count, _ := message.Get("retry_count").Int()

	return retryEnabled && count < max
}

func incrementRetry(message *Msg) (retryCount int) {
	retryCount = 0
	now := time.Now().UTC().Format(LAYOUT)

	// If retry_count hasn't been set, then this is our first failure so indicate it
	if count, err := message.Get("retry_count").Int(); err != nil {
		message.Set("failed_at", now)
	} else {
		message.Set("retried_at", now)
		retryCount = count + 1
	}

	message.Set("retry_count", retryCount)
	return
}

func secondsToDelay(count int) int {
	power := math.Pow(float64(count), 4)
	return int(power) + 15 + (rand.Intn(30) * (count + 1))
}
