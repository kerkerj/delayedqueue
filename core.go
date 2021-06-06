package delayedqueue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
	"go.uber.org/zap"
)

const (

	// defaultRedisKeyForDelayedQueue redis key of the delayed queue
	defaultRedisKeyForDelayedQueue = "delayed_v1"
	// defaultRedisKeyForWorkingQueue redis key of the working queue
	defaultRedisKeyForWorkingQueue = "working_v1"

	// defaultPollingCount is the number of pulling items from Redis delayed queue on each time
	defaultPollingCount = 10
	// defaultPollingInterval is the interval of pulling items from Redis
	defaultPollingInterval = time.Second

	// defaultWorkerCount is the number of workers (goroutines)
	defaultWorkerCount = 4
	// DefaultMaxPopCount is the number of pulling items from Redis worker queue on each time
	DefaultMaxPopCount = 100
)

var (
	// Use jsoniter
	json = jsoniter.ConfigCompatibleWithStandardLibrary

	// zRangeByScoreAndZRemByTS calls `zrangebyscore` and `zrem`
	zRangeByScoreAndZRemByTS = redis.NewScript(`
local message = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2]);
if #message > 0 then
  redis.call('ZREM', KEYS[1], unpack(message));
  return message;
else
  return nil;
end`)

	errExceedMaxPopCount   = errors.New("the given count params exceed the DefaultMaxPopCount")
	errCastDelayedQueueMsg = errors.New("can not cast the delayed queue data from Redis")
	errEmptyQueueName      = errors.New("empty queue name")
	errNilDelayedItem      = errors.New("given delayed item is nil")
)

// Service is the delayed queue service
type Service struct {
	redisClient                *redis.Client
	redisKeyForDelayedQueue    string // redis key of the delayed queue
	redisKeyForWorkingQueue    string // redis key of the working queue
	workerCountForWorkingQueue int    // numbers of go-routine (for each registered working queue) will be created to consume from the working queue

	pollingInterval time.Duration // waiting interval when there is no data to retrieve
	pollingCount    int           // how many items are being taken from the delayed queue at a time

	// registeredWorkingActions working queues are white-listed, along with working function
	registeredWorkingActions map[string]func(string) error

	logger     *zap.Logger
	background *sync.Once
}

// New initializes the delayed queue
func New(client *redis.Client, options ...ServiceOption) *Service {
	logger, _ := zap.NewDevelopment()
	s := &Service{
		redisClient:                client,
		redisKeyForDelayedQueue:    defaultRedisKeyForDelayedQueue,
		redisKeyForWorkingQueue:    defaultRedisKeyForWorkingQueue,
		workerCountForWorkingQueue: defaultWorkerCount,
		pollingInterval:            defaultPollingInterval,
		pollingCount:               defaultPollingCount,
		registeredWorkingActions:   make(map[string]func(string) error),
		logger:                     logger,
		background:                 &sync.Once{},
	}
	for _, option := range options {
		if option != nil {
			option(s)
		}
	}
	return s
}

// ServiceOption allows to customize the service's options
type ServiceOption func(s *Service)

// RegisterWorkingAction registers the working actions
func (s *Service) RegisterWorkingAction(name string, action func(string) error) {
	s.logger.Sugar().Infof("delayedqueue: Registered action: %s", name)
	s.registeredWorkingActions[name] = action
}

// RunBackgroundLoop runs goroutines (once) to process the delayed queue and the working queue
func (s *Service) RunBackgroundLoop() {
	s.background.Do(func() {
		// pollDelayedQueue
		go func() {
			s.logger.Sugar().Infof("delayedqueue: DelayedQueue starts working, pollingCount: %d", s.pollingCount)
			for {
				waitingInterval := s.pollDelayedQueue(context.Background())
				time.Sleep(waitingInterval)
			}
		}()

		// pollWorkingQueue
		for i := 0; i < s.workerCountForWorkingQueue; i++ {
			s.logger.Sugar().Infof("delayedqueue: WorkQueue[%d] starts working", i)
			go func() {
				for {
					waitingInterval := s.pollWorkingQueue(context.Background())
					time.Sleep(waitingInterval)
				}
			}()
		}
	})
}

// WithRedisKeyDelayedQueue overwrites the redis key of delayed queue
func WithRedisKeyDelayedQueue(name string) ServiceOption {
	return func(s *Service) {
		s.redisKeyForDelayedQueue = name
	}
}

// WithRedisKeyWorkingQueue overwrites the redis key of working queue
func WithRedisKeyWorkingQueue(name string) ServiceOption {
	return func(s *Service) {
		s.redisKeyForWorkingQueue = name
	}
}

// WithWorkerCount overwrites the worker count (the number of goroutines which poll items from working queue)
func WithWorkerCount(workerCount int) ServiceOption {
	return func(s *Service) {
		s.workerCountForWorkingQueue = workerCount
	}
}

// WithPollingInterval overwrites the polling interval of delayed queue
func WithPollingInterval(interval time.Duration) ServiceOption {
	return func(s *Service) {
		s.pollingInterval = interval
	}
}

// WithPollingCount overwrites the polling count of delayed queue
func WithPollingCount(count int) ServiceOption {
	return func(s *Service) {
		s.pollingCount = count
	}
}

// WithCustomZapLogger allows to use custom zap logger
func WithCustomZapLogger(logger *zap.Logger) ServiceOption {
	return func(s *Service) {
		s.logger = logger
	}
}

// Close closes Service gracefully
func (s *Service) Close() error {
	// TODO implement
	return nil
}

// pollWorkingQueue polls data from the working queue every interval
func (s *Service) pollWorkingQueue(ctx context.Context) (waitingInterval time.Duration) {
	itemJSONStr, err := s.dequeueFromWorkingQueue(ctx, s.redisKeyForWorkingQueue)
	if err != nil {
		s.logger.Sugar().Errorf("delayedqueue: dequeueFromWorkingQueue error: %s", err.Error())
	}

	if itemJSONStr == "" {
		// s.logger.Sugar().Debugf("delayedqueue: There is no item in the working queue, sleep")
		return s.pollingInterval
	}

	if err := s.work(itemJSONStr); err != nil {
		s.logger.Sugar().Errorf("delayedqueue: work error: %s", err.Error())
	}
	return 0
}

// work takes the json string in redis and executes it
func (s *Service) work(itemJSONStr string) error {
	workingItem := &WorkingItem{}
	if err := json.Unmarshal([]byte(itemJSONStr), workingItem); err != nil {
		return fmt.Errorf("unmarshal workingItem (%s) error: %w", itemJSONStr, err)
	}
	s.logger.Sugar().Debugf("delayedqueue: workingItem: %+v", workingItem)

	f, ok := s.registeredWorkingActions[workingItem.FuncName]
	if !ok {
		return fmt.Errorf("unregistered func name, workingItem (%s)", itemJSONStr)
	}
	return f(workingItem.ArgsJSONStr)
}

// pollDelayedQueue polls data from the delayed queue every interval, and enqueues data to the working queue
func (s *Service) pollDelayedQueue(ctx context.Context) (waitingInterval time.Duration) {
	// Pop data from delayed queue
	readyToQueueItems, err := s.popMatchedDelayedItems(ctx, time.Now(), s.pollingCount)
	if err != nil {
		s.logger.Sugar().Errorf("delayedqueue: popMatchedDelayedItems error: %s", err.Error())
	}

	// Sleep a while if there is nothing
	if len(readyToQueueItems) == 0 {
		// s.logger.Debug("delayedqueue: popMatchedDelayedItems no item, sleep")
		return s.pollingInterval
	}
	s.logger.Sugar().Debugf("delayedqueue: popMatchedDelayedItems success, count: %d", len(readyToQueueItems))

	// Enqueue data to working queues
	if err := s.enqueueToWorkingQueue(ctx, s.redisKeyForWorkingQueue, readyToQueueItems...); err != nil {
		s.logger.Sugar().Errorf("delayedqueue enqueueToWorkingQueue error: %s", err.Error())
	}
	s.logger.Debug("delayedqueue: enqueueToWorkingQueue success")
	return 0
}

// popMatchedDelayedItems returns the working queue items from the delayed queue
func (s *Service) popMatchedDelayedItems(ctx context.Context, t time.Time, count int) ([]interface{}, error) {
	if count > DefaultMaxPopCount {
		return nil, errExceedMaxPopCount
	}

	items, err := zRangeByScoreAndZRemByTS.Run(ctx, s.redisClient, []string{s.redisKeyForDelayedQueue}, t.UnixNano(), count).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}

	workingQueueItems, ok := items.([]interface{})
	if !ok {
		return nil, errCastDelayedQueueMsg
	}
	return workingQueueItems, nil
}

// PutDelayedItems adds the given DelayedItem the the delayed queue
// Note: It will return error and will not add all of the data if there is any error happened when marshaling the items.
func (s *Service) PutDelayedItems(ctx context.Context, delayedItems ...*DelayedItem) error {
	members := make([]*redis.Z, len(delayedItems))
	for i := range delayedItems {
		if delayedItems[i] == nil {
			return errNilDelayedItem
		}
		preparedItem, err := json.Marshal(&delayedItems[i].WorkingItem)
		if err != nil {
			return err // if one fails, then all fail
		}
		members[i] = &redis.Z{Score: float64(delayedItems[i].ExecuteAt), Member: preparedItem}
	}
	s.logger.Sugar().Debugf("delayedqueue: enqueue to delayed queue [%s], data: %+v", s.redisKeyForDelayedQueue, members)
	return s.redisClient.ZAdd(ctx, s.redisKeyForDelayedQueue, members...).Err()
}

// enqueueToWorkingQueue enqueue data to the working queue
func (s *Service) enqueueToWorkingQueue(ctx context.Context, queueName string, readyItems ...interface{}) error {
	if queueName == "" {
		return errEmptyQueueName
	}
	return s.redisClient.RPush(ctx, queueName, readyItems...).Err()
}

// dequeueFromWorkingQueue dequeue data from the working queue
func (s *Service) dequeueFromWorkingQueue(ctx context.Context, queueName string) (string, error) {
	result, err := s.redisClient.LPop(ctx, queueName).Result()
	if errors.Is(err, redis.Nil) {
		return result, nil
	}
	return result, err
}
