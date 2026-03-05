package raft

import (
	"encoding/json"
	"raft/pkg/asserts"
	"testing"
)

func TestStateMachineApply(t *testing.T) {
	storage := &KVStorage{state: map[string]string{}}
	sm := NewStateMachine(storage)
	setx1cmd := Command{ActionSet, "x", "1"}
	setx1cmdJson, err := json.Marshal(&setx1cmd)
	asserts.ErrNil(t, err)
	setx3cmd := Command{ActionSet, "x", "3"}
	setx3cmdJson, err := json.Marshal(&setx3cmd)
	asserts.ErrNil(t, err)
	sety9cmd := Command{ActionSet, "y", "9"}
	sety9cmdJson, err := json.Marshal(&sety9cmd)
	asserts.ErrNil(t, err)
	delxcmd := Command{ActionDelete, "x", ""}
	delxcmdJson, err := json.Marshal(&delxcmd)
	asserts.ErrNil(t, err)
	setx1 := Entry{setx1cmdJson, 1}
	setx3 := Entry{setx3cmdJson, 1}
	sety9 := Entry{sety9cmdJson, 1}
	delx := Entry{delxcmdJson, 1}
	sm.AppendLogs(setx1, setx3, sety9, delx)
	sm.Apply(0)
	asserts.Equal(t, "1", string(storage.MustGet("x")))
	sm.Apply(1)
	asserts.Equal(t, "3", string(storage.MustGet("x")))
	sm.Apply(3)
	_, hasX := storage.Get("x")
	asserts.False(t, hasX)
	asserts.Equal(t, "9", string(storage.MustGet("y")))
}
