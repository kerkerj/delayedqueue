package example

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"

	"github.com/kerkerj/delayedqueue"
)

func ExampleService() {
	// set redis client (we use miniredis here for simplicity)
	mockRedis, _ := miniredis.Run()
	redisClient := redis.NewClient(&redis.Options{Addr: mockRedis.Addr()})

	// init delayedqueue
	service := delayedqueue.New(redisClient,
		// optional
		delayedqueue.WithRedisKeyDelayedQueue("delayed"),
		delayedqueue.WithRedisKeyWorkingQueue("working"),
		delayedqueue.WithPollingInterval(1*time.Second),
		delayedqueue.WithPollingCount(10),
		delayedqueue.WithWorkerCount(4),
		delayedqueue.WithCustomZapLogger(zap.NewNop()), // no zap log output
	)

	// Register
	// use executionCount to verify whether the job is executed or not
	executionCount := int32(0)
	countDataFuncName := "CountData"
	service.RegisterWorkingAction(countDataFuncName, func(_ string) error {
		atomic.AddInt32(&executionCount, 1)
		return nil
	})

	// Start to run goroutines
	service.RunBackgroundLoop()

	// make 10 DelayedItems
	enqueueData := make([]*delayedqueue.DelayedItem, 10)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 10; i++ {
		enqueueData[i] = &delayedqueue.DelayedItem{
			ExecuteAt: time.Now().Add(-10 * time.Second).UnixNano(),
			WorkingItem: delayedqueue.WorkingItem{
				QueueName:   "working_v1",
				FuncName:    countDataFuncName,
				ArgsJSONStr: fmt.Sprintf(`{"args1":"%d"}`, rand.Intn(1000)),
			},
		}
	}
	service.PutDelayedItems(context.Background(), enqueueData...)

	// wait workers to process jobs
	time.Sleep(1200 * time.Millisecond)

	// Assert
	fmt.Println(executionCount)
	// Output:
	// 10
}
