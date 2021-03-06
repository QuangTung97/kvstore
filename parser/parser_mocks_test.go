// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package parser

import (
	"sync"
)

// Ensure, that CommandHandlerMock does implement CommandHandler.
// If this is not the case, regenerate this file with moq.
var _ CommandHandler = &CommandHandlerMock{}

// CommandHandlerMock is a mock implementation of CommandHandler.
//
// 	func TestSomethingThatUsesCommandHandler(t *testing.T) {
//
// 		// make and configure a mocked CommandHandler
// 		mockedCommandHandler := &CommandHandlerMock{
// 			OnDELFunc: func(key []byte)  {
// 				panic("mock out the OnDEL method")
// 			},
// 			OnLGETFunc: func(key []byte)  {
// 				panic("mock out the OnLGET method")
// 			},
// 			OnLSETFunc: func(key []byte, lease uint32, value []byte)  {
// 				panic("mock out the OnLSET method")
// 			},
// 		}
//
// 		// use mockedCommandHandler in code that requires CommandHandler
// 		// and then make assertions.
//
// 	}
type CommandHandlerMock struct {
	// OnDELFunc mocks the OnDEL method.
	OnDELFunc func(key []byte)

	// OnLGETFunc mocks the OnLGET method.
	OnLGETFunc func(key []byte)

	// OnLSETFunc mocks the OnLSET method.
	OnLSETFunc func(key []byte, lease uint32, value []byte)

	// calls tracks calls to the methods.
	calls struct {
		// OnDEL holds details about calls to the OnDEL method.
		OnDEL []struct {
			// Key is the key argument value.
			Key []byte
		}
		// OnLGET holds details about calls to the OnLGET method.
		OnLGET []struct {
			// Key is the key argument value.
			Key []byte
		}
		// OnLSET holds details about calls to the OnLSET method.
		OnLSET []struct {
			// Key is the key argument value.
			Key []byte
			// Lease is the lease argument value.
			Lease uint32
			// Value is the value argument value.
			Value []byte
		}
	}
	lockOnDEL  sync.RWMutex
	lockOnLGET sync.RWMutex
	lockOnLSET sync.RWMutex
}

// OnDEL calls OnDELFunc.
func (mock *CommandHandlerMock) OnDEL(key []byte) {
	if mock.OnDELFunc == nil {
		panic("CommandHandlerMock.OnDELFunc: method is nil but CommandHandler.OnDEL was just called")
	}
	callInfo := struct {
		Key []byte
	}{
		Key: key,
	}
	mock.lockOnDEL.Lock()
	mock.calls.OnDEL = append(mock.calls.OnDEL, callInfo)
	mock.lockOnDEL.Unlock()
	mock.OnDELFunc(key)
}

// OnDELCalls gets all the calls that were made to OnDEL.
// Check the length with:
//     len(mockedCommandHandler.OnDELCalls())
func (mock *CommandHandlerMock) OnDELCalls() []struct {
	Key []byte
} {
	var calls []struct {
		Key []byte
	}
	mock.lockOnDEL.RLock()
	calls = mock.calls.OnDEL
	mock.lockOnDEL.RUnlock()
	return calls
}

// OnLGET calls OnLGETFunc.
func (mock *CommandHandlerMock) OnLGET(key []byte) {
	if mock.OnLGETFunc == nil {
		panic("CommandHandlerMock.OnLGETFunc: method is nil but CommandHandler.OnLGET was just called")
	}
	callInfo := struct {
		Key []byte
	}{
		Key: key,
	}
	mock.lockOnLGET.Lock()
	mock.calls.OnLGET = append(mock.calls.OnLGET, callInfo)
	mock.lockOnLGET.Unlock()
	mock.OnLGETFunc(key)
}

// OnLGETCalls gets all the calls that were made to OnLGET.
// Check the length with:
//     len(mockedCommandHandler.OnLGETCalls())
func (mock *CommandHandlerMock) OnLGETCalls() []struct {
	Key []byte
} {
	var calls []struct {
		Key []byte
	}
	mock.lockOnLGET.RLock()
	calls = mock.calls.OnLGET
	mock.lockOnLGET.RUnlock()
	return calls
}

// OnLSET calls OnLSETFunc.
func (mock *CommandHandlerMock) OnLSET(key []byte, lease uint32, value []byte) {
	if mock.OnLSETFunc == nil {
		panic("CommandHandlerMock.OnLSETFunc: method is nil but CommandHandler.OnLSET was just called")
	}
	callInfo := struct {
		Key   []byte
		Lease uint32
		Value []byte
	}{
		Key:   key,
		Lease: lease,
		Value: value,
	}
	mock.lockOnLSET.Lock()
	mock.calls.OnLSET = append(mock.calls.OnLSET, callInfo)
	mock.lockOnLSET.Unlock()
	mock.OnLSETFunc(key, lease, value)
}

// OnLSETCalls gets all the calls that were made to OnLSET.
// Check the length with:
//     len(mockedCommandHandler.OnLSETCalls())
func (mock *CommandHandlerMock) OnLSETCalls() []struct {
	Key   []byte
	Lease uint32
	Value []byte
} {
	var calls []struct {
		Key   []byte
		Lease uint32
		Value []byte
	}
	mock.lockOnLSET.RLock()
	calls = mock.calls.OnLSET
	mock.lockOnLSET.RUnlock()
	return calls
}
