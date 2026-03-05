package main

import (
	"encoding/json"
	"fmt"
)

type RequestVote struct {
	Term         int
	CandidateId  NodeId
	LastLogIndex int
	LastLogTerm  int
}

// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2,§5.4)
type RequestVoteResult struct {
	Term        int
	VoteGranted bool
}

type AppendEntries struct {
	Term         int
	LeaderId     NodeId
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
type AppendEntriesResult struct {
	Term    int
	Success bool
}

type NodeId int

const EmptyId NodeId = -1

func (id NodeId) String() string {
	if id == EmptyId {
		return "None"
	}
	return fmt.Sprintf("node-%d", id)
}

type State int

func (s State) MarshalJSON() ([]byte, error) {
	return []byte(`"` + s.String() + `"`), nil
}

func (s *State) UnmarshalJSON(p []byte) error {
	if string(p) == `"Follower"` {
		*s = Follower
		return nil
	}
	if string(p) == `"Candidate"` {
		*s = Candidate
		return nil
	}
	if string(p) == `"Leader"` {
		*s = Leader
		return nil
	}
	intvalue := int(*s)
	err := json.Unmarshal(p, &intvalue)
	*s = State(intvalue)
	return err
}

func (s State) String() string {
	if s == Follower {
		return "Follower"
	}
	if s == Candidate {
		return "Candidate"
	}
	if s == Leader {
		return "Leader"
	}
	return "UNKNOWN"
}

const (
	Follower  State = iota
	Candidate State = iota
	Leader    State = iota
)
