package main_test

import (
	. "hyperraft"
	"hyperraft/pkg/asserts"
	"testing"
)

func TestStateMachineApply(t *testing.T) {
	sm := NewStateMachine()
	setx1 := Entry{Command{ActionSet, "x", []byte("1")}, 1}
	setx3 := Entry{Command{ActionSet, "x", []byte("3")}, 1}
	sety9 := Entry{Command{ActionSet, "y", []byte("9")}, 1}
	delx := Entry{Command{ActionDelete, "x", nil}, 1}
	sm.AppendLogs(setx1, setx3, sety9, delx)
	sm.Apply(0)
	asserts.Equal(t, "1", string(sm.MustGet("x")))
	sm.Apply(1)
	asserts.Equal(t, "3", string(sm.MustGet("x")))
	sm.Apply(3)
	_, hasX := sm.Get("x")
	asserts.False(t, hasX)
	asserts.Equal(t, "9", string(sm.MustGet("y")))
}
