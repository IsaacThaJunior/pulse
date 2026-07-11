package redisqueue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisQueue(t *testing.T) {
	client, cleanup := setupTestRedis(t)
	defer cleanup()

	queue := NewRedisQueue(client)
	ctx := context.Background()

	t.Run("Enqueue and Dequeue", func(t *testing.T) {
		client.Del(ctx, "events_high", "events_low")

		err := queue.EnqueueWithPriority("task1", "high")
		require.NoError(t, err)

		err = queue.EnqueueWithPriority("task2", "low")
		require.NoError(t, err)

		task, _, err := queue.DequeuePriorityBlocking(5 * time.Second)
		require.NoError(t, err)
		assert.Equal(t, "task1", task)

		task, _, err = queue.DequeuePriorityBlocking(5 * time.Second)
		require.NoError(t, err)
		assert.Equal(t, "task2", task)
	})

	t.Run("Dequeue from empty queue", func(t *testing.T) {
		client.Del(ctx, "events_high", "events_medium", "events_low")
		emptyQueue := NewRedisQueue(client)

		task, _, err := emptyQueue.DequeuePriorityBlocking(5 * time.Second)
		require.NoError(t, err)
		assert.Equal(t, "", task)
	})

	t.Run("EnqueueToDLQ", func(t *testing.T) {
		client.Del(ctx, "dead_letter_queue")

		err := queue.EnqueueToDLQ("failed_task")
		require.NoError(t, err)

		length, err := client.LLen(ctx, "dead_letter_queue").Result()
		require.NoError(t, err)
		assert.Equal(t, int64(1), length)
	})

	t.Run("Concurrent operations", func(t *testing.T) {
		client.Del(ctx, "events_medium")
		concurrentQueue := NewRedisQueue(client)

		const numTasks = 100
		var wg sync.WaitGroup

		for i := range numTasks {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				err := concurrentQueue.EnqueueWithPriority(string(rune(id)), "medium")
				assert.NoError(t, err)
			}(i)
		}

		wg.Wait()

		depths, err := concurrentQueue.GetQueueDepths()
		require.NoError(t, err)
		assert.Equal(t, int64(numTasks), depths["medium"])
	})

}
