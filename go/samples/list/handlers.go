package list

import (
	"errors"
	"slices"

	"github.com/mchandschuh/temporal-actors/go/src/actor"
)

var ErrIndexOutOfRange = errors.New("list index out of range")

func AddElement(_ actor.Context, state State, message Add) State {
	next := clone(state, len(state)+1)
	return append(next, message.Element)
}

func RemoveElement(_ actor.Context, state State, message Remove) State {
	index := slices.Index(state, message.Element)
	if index < 0 {
		return state
	}
	next := make(State, 0, len(state)-1)
	next = append(next, state[:index]...)
	return append(next, state[index+1:]...)
}

func RemoveAtElement(_ actor.Context, state State, message RemoveAt) State {
	if !validIndex(state, message.Index) {
		return state
	}
	next := make(State, 0, len(state)-1)
	next = append(next, state[:message.Index]...)
	return append(next, state[message.Index+1:]...)
}

func SetElement(_ actor.Context, state State, message Set) State {
	if !validIndex(state, message.Index) {
		return state
	}
	next := clone(state, len(state))
	next[message.Index] = message.Element
	return next
}

func ClearList(_ actor.Context, state State, _ Clear) State {
	if len(state) == 0 {
		return state
	}
	return State{}
}

func GetList(_ actor.Context, state State, _ Get) (State, error) {
	return clone(state, len(state)), nil
}

func GetAtElement(_ actor.Context, state State, message GetAt) (string, error) {
	if !validIndex(state, message.Index) {
		return "", ErrIndexOutOfRange
	}
	return state[message.Index], nil
}

func clone(state State, capacity int) State {
	next := make(State, len(state), capacity)
	copy(next, state)
	return next
}

func validIndex(state State, index int) bool {
	return index >= 0 && index < len(state)
}
