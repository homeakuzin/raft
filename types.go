package main

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
	Entries      []byte
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

const EmptyId NodeId = 9999

type State int

const (
	Follower  State = iota
	Candidate State = iota
	Leader    State = iota
)
