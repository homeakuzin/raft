package raft

import (
	"encoding/json"
	"raft/pkg/asserts"
	"raft/storage"
	"testing"
)

func TestStateMachineApply(t *testing.T) {
	kv := storage.NewKVStorage()
	sm := NewStateMachine(kv)
	setx1cmd := storage.Command{storage.ActionSet, "x", "1"}
	setx1cmdJson, err := json.Marshal(&setx1cmd)
	asserts.ErrNil(t, err)
	setx3cmd := storage.Command{storage.ActionSet, "x", "3"}
	setx3cmdJson, err := json.Marshal(&setx3cmd)
	asserts.ErrNil(t, err)
	sety9cmd := storage.Command{storage.ActionSet, "y", "9"}
	sety9cmdJson, err := json.Marshal(&sety9cmd)
	asserts.ErrNil(t, err)
	delxcmd := storage.Command{storage.ActionDelete, "x", ""}
	delxcmdJson, err := json.Marshal(&delxcmd)
	asserts.ErrNil(t, err)
	setx1 := Entry{setx1cmdJson, 1}
	setx3 := Entry{setx3cmdJson, 1}
	sety9 := Entry{sety9cmdJson, 1}
	delx := Entry{delxcmdJson, 1}
	sm.AppendLogs(setx1, setx3, sety9, delx)
	sm.Apply(0)
	asserts.Equal(t, "1", string(kv.MustGet("x")))
	sm.Apply(1)
	asserts.Equal(t, "3", string(kv.MustGet("x")))
	sm.Apply(3)
	_, hasX := kv.Get("x")
	asserts.False(t, hasX)
	asserts.Equal(t, "9", string(kv.MustGet("y")))
}
