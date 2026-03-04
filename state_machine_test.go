package main_test

import (
	. "hyperraft"
	"hyperraft/pkg/asserts"
	"testing"
)

func TestStateMachineApply(t *testing.T) {
	sm := NewStateMachine()
	setx1 := Command{ActionSet, "x", []byte("1")}
	setx3 := Command{ActionSet, "x", []byte("3")}
	sety9 := Command{ActionSet, "y", []byte("9")}
	delx := Command{ActionDelete, "x", nil}
	sm.Apply(setx1)
	asserts.Equal(t, "1", string(sm.State["x"]))
	sm.Apply(setx3)
	asserts.Equal(t, "3", string(sm.State["x"]))
	sm.Apply(sety9)
	asserts.Equal(t, "9", string(sm.State["y"]))
	sm.Apply(delx)
	asserts.NotHasKey(t, "x", sm.State)
	asserts.Equal(t, "9", string(sm.State["y"]))
}

func TestStateMachineRestore(t *testing.T) {
	sm := NewStateMachine()
	setx1 := Command{ActionSet, "x", []byte("1")}
	setx3 := Command{ActionSet, "x", []byte("3")}
	sety9 := Command{ActionSet, "y", []byte("9")}
	delx := Command{ActionDelete, "x", nil}
	sm.Logs = []Command{setx1, setx3, sety9, delx}
	sm.Restore()
	asserts.NotHasKey(t, "x", sm.State)
	asserts.Equal(t, "9", string(sm.State["y"]))
}
