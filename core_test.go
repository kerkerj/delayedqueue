package delayedqueue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func setup() (*miniredis.Miniredis, *Service, *observer.ObservedLogs) {
	// zap observer
	core, recordedLogs := observer.New(zapcore.DebugLevel)

	mockRedis, _ := miniredis.Run()
	redisClient := redis.NewClient(&redis.Options{Addr: mockRedis.Addr()})

	s := &Service{
		redisClient:                redisClient,
		redisKeyForDelayedQueue:    defaultRedisKeyForDelayedQueue,
		redisKeyForWorkingQueue:    defaultRedisKeyForWorkingQueue,
		workerCountForWorkingQueue: defaultWorkerCount,
		pollingInterval:            defaultPollingInterval,
		pollingCount:               defaultPollingCount,
		registeredWorkingActions:   make(map[string]func(string) error),
		logger:                     zap.New(core),
		background:                 &sync.Once{},
	}

	return mockRedis, s, recordedLogs
}

func TestNew(t *testing.T) {
	Convey("TestNew", t, func() {
		// Arrange
		mockRedis, _ := miniredis.Run()
		defer mockRedis.Close()
		redisClient := redis.NewClient(&redis.Options{Addr: mockRedis.Addr()})

		// Act
		s := New(redisClient)

		// Assert
		So(s, ShouldNotBeNil)
	})
}

func TestService_RunBackgroundLoop(t *testing.T) {
	Convey("TestService_RunBackgroundLoop", t, func() {
		// zap observer
		core, recordedLogs := observer.New(zapcore.DebugLevel)

		mockRedis, _ := miniredis.Run()
		defer mockRedis.Close()
		redisClient := redis.NewClient(&redis.Options{Addr: mockRedis.Addr()})

		// Arrange
		s := New(redisClient, WithCustomZapLogger(zap.New(core)))

		// Act & Assert
		So(func() {
			s.RunBackgroundLoop()
		}, ShouldNotPanic)
		So(recordedLogs.Len(), ShouldBeGreaterThan, 0)
		t.Log(recordedLogs.Len())
	})
}

func TestService_RegisterWorkingAction(t *testing.T) {
	Convey("TestService_RegisterWorkingAction", t, func() {
		// Arrange
		s := &Service{registeredWorkingActions: map[string]func(string) error{}, logger: zap.NewExample()}

		// Act
		s.RegisterWorkingAction("SendData", func(_ string) error {
			return errors.New("error")
		})

		// Assert
		So(s.registeredWorkingActions["SendData"], ShouldNotBeNil)
	})
}

func TestService_pollWorkingQueue(t *testing.T) {
	Convey("TestService_pollWorkingQueue", t, func() {
		mockRedis, service, recordedLogs := setup()
		defer mockRedis.Close()

		Convey("redis error", func() {
			// Arrange
			mockRedis.SetError("unexpected error")

			// Act
			waitingInterval := service.pollWorkingQueue(context.Background())

			// Assert
			So(waitingInterval, ShouldEqual, time.Second)
			expectedLog := "delayedqueue: dequeueFromWorkingQueue error: unexpected error"
			So(recordedLogs.FilterMessage(expectedLog).Len(), ShouldEqual, 1)
		})

		Convey("No item", func() {
			// Act
			waitingInterval := service.pollWorkingQueue(context.Background())

			// Assert
			So(waitingInterval, ShouldEqual, time.Second)
		})

		Convey("work (unregistered working queue)", func() {
			// Arrange
			mockRedis.RPush("working_v1", `["working_v1", "1620616935530101000", "CallAPI", "{\"arg1\":\"1\"}"]`)

			// Act
			waitingInterval := service.pollWorkingQueue(context.Background())

			// Assert
			So(waitingInterval, ShouldEqual, 0)
			expectedLog := "delayedqueue: work error: unregistered func name, workingItem ([\"working_v1\", \"1620616935530101000\", \"CallAPI\", \"{\\\"arg1\\\":\\\"1\\\"}\"])"
			So(recordedLogs.FilterMessage(expectedLog).Len(), ShouldEqual, 1)
		})
	})
}

func TestService_work(t *testing.T) {
	Convey("TestService_pollWorkingQueue", t, func() {
		mockRedis, service, _ := setup()
		defer mockRedis.Close()

		Convey("json unmarshal error", func() {
			// Arrange
			invalid := `{"hello":"123"}`

			// Act
			err := service.work(invalid)

			// Assert
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldStartWith, "unmarshal workingItem ({\"hello\":\"123\"}) error")
		})

		Convey("unregistered func", func() {
			// Arrange
			unregistered := `["working_v1", "CallAPI", "{\"arg1\":\"1\"}"]`

			// Act
			err := service.work(unregistered)

			// Assert
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldStartWith, "unregistered func name, workingItem ([\"working_v1\", \"CallAPI\", \"{\\\"arg1\\\":\\\"1\\\"}\"])")
		})

		Convey("registered func", func() {
			item := `["working_v1", "CallAPI", "{\"arg1\":\"1\"}"]`

			Convey("error", func() {
				// Arrange
				service.registeredWorkingActions["CallAPI"] = func(s string) error {
					return errors.New("call API error")
				}

				// Act
				err := service.work(item)

				// Assert
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "call API error")
			})

			Convey("success", func() {
				// Arrange
				callAPI := false
				service.registeredWorkingActions["CallAPI"] = func(s string) error { // nolint:unparam
					callAPI = true
					return nil
				}

				// Act
				err := service.work(item)

				// Assert
				So(err, ShouldBeNil)
				So(callAPI, ShouldBeTrue)
			})
		})
	})
}

func TestService_pollDelayedQueue(t *testing.T) {
	Convey("TestService_pollDelayedQueue", t, func() {
		mockRedis, service, recordedLogs := setup()
		defer mockRedis.Close()

		Convey("popMatchedDelayedItems error (pollingCount is too big) and `no data`", func() {
			// Arrange
			service.pollingCount = 1000000

			// Act
			interval := service.pollDelayedQueue(context.Background())

			// Assert
			So(interval, ShouldEqual, time.Second)
			So(recordedLogs.FilterMessage(
				"delayedqueue: popMatchedDelayedItems error: the given count params exceed the DefaultMaxPopCount",
			).Len(), ShouldEqual, 1)
		})

		Convey("enqueueToWorkingQueue error", func() {
			// Arrange
			mockRedis.ZAdd("delayed_v1", float64(time.Now().UnixNano()-10000), "[\"working_v1\", \"1620616935530101000\", \"CallAPI\", \"{\\\"arg1\\\":\\\"1\\\"}\"]")
			service.redisKeyForWorkingQueue = ""

			// Act
			interval := service.pollDelayedQueue(context.Background())

			// Assert
			So(interval, ShouldEqual, 0)
			So(recordedLogs.FilterMessage(
				"delayedqueue enqueueToWorkingQueue error: empty queue name",
			).Len(), ShouldEqual, 1)
		})

		Convey("success", func() {
			// Arrange
			mockRedis.ZAdd("delayed_v1", float64(time.Now().UnixNano()-10000), "[\"working_v1\", \"1620616935530101000\", \"CallAPI\", \"{\\\"arg1\\\":\\\"1\\\"}\"]")

			// Act
			interval := service.pollDelayedQueue(context.Background())

			// Assert
			So(interval, ShouldEqual, 0)
			So(recordedLogs.FilterMessage(
				"delayedqueue: enqueueToWorkingQueue success",
			).Len(), ShouldEqual, 1)
		})
	})
}

func TestService_enqueueToWorkingQueue(t *testing.T) {
	Convey("TestService_enqueueToWorkingQueue", t, func() {
		mockRedis, service, _ := setup()
		defer mockRedis.Close()

		Convey("no error", func() {
			// Act
			err := service.enqueueToWorkingQueue(context.Background(), "queue", nil)

			// Assert
			So(err, ShouldBeNil)
		})

		Convey("has error", func() {
			// Act
			err := service.enqueueToWorkingQueue(context.Background(), "", nil)

			// Assert
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "empty queue name")
		})
	})
}

func TestService_popMatchedDelayedItems(t *testing.T) {
	Convey("TestService_popMatchedDelayedItems", t, func() {
		mockRedis, service, _ := setup()
		defer mockRedis.Close()

		Convey("given count is bigger than the DefaultMaxPopCount", func() {
			// Act
			_, err := service.popMatchedDelayedItems(context.Background(), time.Now(), 1000000)

			// Assert
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "the given count params exceed the DefaultMaxPopCount")
		})

		Convey("zRangeByScoreAndZRemByTS", func() {
			Convey("redis.Nil", func() {
				// Act
				data, err := service.popMatchedDelayedItems(context.Background(), time.Now(), 1)

				// Assert
				So(len(data), ShouldEqual, 0)
				So(err, ShouldBeNil)
			})

			Convey("redis error", func() {
				// Arrange
				mockRedis.SetError("unexpected error")

				// Act
				data, err := service.popMatchedDelayedItems(context.Background(), time.Now(), 1)

				// Assert
				So(len(data), ShouldEqual, 0)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "unexpected error")
			})
		})

		Convey("success", func() {
			// Arrange
			mockRedis.ZAdd("delayed_v1", float64(time.Now().UnixNano()-10000), "")

			// Act
			data, err := service.popMatchedDelayedItems(context.Background(), time.Now(), 1)

			// Assert
			So(len(data), ShouldEqual, 1)
			So(err, ShouldBeNil)
		})
	})
}

func TestService_PutDelayedItems(t *testing.T) {
	Convey("TestService_PutDelayedItems", t, func() {
		mockRedis, service, _ := setup()
		defer mockRedis.Close()

		Convey("success", func() {
			// Act
			err := service.PutDelayedItems(context.Background(), &DelayedItem{
				ExecuteAt: time.Now().UnixNano(),
				WorkingItem: WorkingItem{
					QueueName:   "queue",
					FuncName:    "CallAPI",
					ArgsJSONStr: `{"args1":"1"}`,
				},
			})

			// Assert
			So(err, ShouldBeNil)
		})

		Convey("error", func() {
			// Act
			err := service.PutDelayedItems(context.Background(), nil)

			// Assert
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "given delayed item is nil")
		})
	})
}

func TestService_WithRedisKeyDelayedQueue(t *testing.T) {
	Convey("TestService_WithRedisKeyDelayedQueue", t, func() {
		// Arrange
		s := &Service{}

		// Act
		WithRedisKeyDelayedQueue("test-delayed")(s)

		// Assert
		So(s.redisKeyForDelayedQueue, ShouldEqual, "test-delayed")
	})
}

func TestService_WithRedisKeyWorkingQueue(t *testing.T) {
	Convey("TestService_WithRedisKeyWorkingQueue", t, func() {
		// Arrange
		s := &Service{}

		// Act
		WithRedisKeyWorkingQueue("test-working")(s)

		// Assert
		So(s.redisKeyForWorkingQueue, ShouldEqual, "test-working")
	})
}

func TestService_WithWorkerCount(t *testing.T) {
	Convey("TestService_WithWorkerCount", t, func() {
		// Arrange
		s := &Service{}

		// Act
		WithWorkerCount(300)(s)

		// Assert
		So(s.workerCountForWorkingQueue, ShouldEqual, 300)
	})
}

func TestService_WithPollingInterval(t *testing.T) {
	Convey("TestService_WithPollingInterval", t, func() {
		// Arrange
		s := &Service{}

		// Act
		WithPollingInterval(10 * time.Second)(s)

		// Assert
		So(s.pollingInterval, ShouldEqual, 10*time.Second)
	})
}

func TestService_WithPollingCount(t *testing.T) {
	Convey("TestService_WithPollingCount", t, func() {
		// Arrange
		s := &Service{}

		// Act
		WithPollingCount(5)(s)

		// Assert
		So(s.pollingCount, ShouldEqual, 5)
	})
}

func TestService_WithCustomZapLogger(t *testing.T) {
	Convey("TestService_WithCustomZapLogger", t, func() {
		// Arrange
		s := &Service{}
		prdLogger, _ := zap.NewProduction()

		// Act
		WithCustomZapLogger(prdLogger)(s)

		// Assert
		So(s.logger, ShouldNotBeNil)
	})
}

func TestService_Close(t *testing.T) {

}
