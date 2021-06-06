package delayedqueue

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestWorkingItem_MarshalJSON(t *testing.T) {
	Convey("TestWorkingItem_MarshalJSON", t, func() {
		// Arrange
		item := &WorkingItem{
			QueueName:   "test",
			FuncName:    "SendData",
			ArgsJSONStr: "{}",
		}

		// Act
		b, err := json.Marshal(item)

		// Assert
		So(string(b), ShouldEqual, `["test","SendData","{}"]`)
		So(err, ShouldBeNil)
	})
}

func TestWorkingItem_UnmarshalJSON(t *testing.T) {
	Convey("TestWorkingItem_UnmarshalJSON", t, func() {
		testCases := []struct {
			name             string
			data             string
			expectedError    bool
			expectedErrorMsg string
		}{
			{
				name:          "success",
				data:          `["working_v1", "CallAPI", "{\"arg1\":\"1\"}"]`,
				expectedError: false,
			},
			{
				name:             "invalid json string",
				data:             "[]",
				expectedError:    true,
				expectedErrorMsg: "unmarshalerDecoder: invalid data, error found in #2 byte of ...|[]|..., bigger context ...|[]|...",
			},
			{
				name:             "unmarshall error",
				data:             `-`,
				expectedError:    true,
				expectedErrorMsg: `readNumberAsString: invalid number, error found in #1 byte of ...|-|..., bigger context ...|-|...`,
			},
		}

		for i := range testCases {
			tt := testCases[i]

			Convey(tt.name, func() {
				// Arrange
				result := &WorkingItem{}

				// Act
				err := json.Unmarshal([]byte(tt.data), result)

				// Assert
				So(err != nil, ShouldEqual, tt.expectedError)
				if tt.expectedError {
					So(err.Error(), ShouldEqual, tt.expectedErrorMsg)
				}
			})
		}
	})
}
