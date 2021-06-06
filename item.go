package delayedqueue

import (
	"errors"
)

const (
	workingItemKeyCount = 3
)

// DelayedItem see WorkingItem
type DelayedItem struct {
	ExecuteAt int64 // ExecuteAt is the time-to-run of this item in UnixNano
	WorkingItem
}

// WorkingItem [queueName, funcName, {"arg1":"1"}]
// Reasons why we use custom json Marshaller:
// 	1. Save space:
// 		`[queueName, funcName, {"arg1":"1"}]` v.s.
// 		`{"queue_name":"working_queue","funcName":"sendData","args":"{\"arg1\":"1"}"}`
// 	2. Easy to define the stored string
type WorkingItem struct {
	QueueName   string // QueueName is the name of the working queue which this item will be enqueued
	FuncName    string // FuncName indicates the func name which this item will be executed with
	ArgsJSONStr string // ArgsJSONStr is the args data in JSON format
}

// MarshalJSON converts WorkingItem to `[queueName, funcName, {"arg1":"1"}]`
func (i *WorkingItem) MarshalJSON() ([]byte, error) {
	strArray := make([]string, 3)
	strArray[0] = i.QueueName
	strArray[1] = i.FuncName
	strArray[2] = i.ArgsJSONStr
	return json.Marshal(strArray)
}

// UnmarshalJSON coverts `[queueName, funcName, {"arg1":"1"}]` to DelayedItem
func (i *WorkingItem) UnmarshalJSON(b []byte) error {
	strArray := make([]string, workingItemKeyCount)
	if err := json.Unmarshal(b, &strArray); err != nil {
		return err
	}

	if len(strArray) < workingItemKeyCount {
		return errors.New("invalid data")
	}

	i.QueueName = strArray[0]
	i.FuncName = strArray[1]
	i.ArgsJSONStr = strArray[2]
	return nil
}
