# Raft Consensus Algorithm Test Suite

## Test Categories

### Leader Election Tests

**Test #1 - Initial Leader Election**
Verify that a cluster of 5 nodes can elect a leader when started. All nodes begin as followers, and within a reasonable timeout period, exactly one node becomes leader and the others recognize it.

**Test #2 - Election Timeout Ranges**
Confirm that election timeouts are randomized across nodes to prevent split votes. No two nodes should have identical election timeouts, and timeouts should fall within the specified range (e.g., 150-300ms).

**Test #3 - Term Incrementation**
Validate that each election cycle increments the term number. When a follower becomes a candidate, its current term should increase by exactly 1, and this term should be propagated through all communication.

**Test #4 - Vote Persistence**
Verify that nodes persist their vote for a given term. Once a node votes for a candidate in a term, it should not vote for any other candidate in the same term, even if it receives another RequestVote RPC.

**Test #5 - Majority Requirement**
Confirm that a candidate only becomes leader if it receives votes from a majority of the cluster (3 out of 5 nodes). If it receives only 2 votes, it should remain a candidate or revert to follower.

**Test #6 - Split Vote Resolution**
Test scenario where multiple nodes become candidates simultaneously, resulting in a split vote. Verify that no node wins the election, all time out, and a new election cycle begins with randomized timeouts.

**Test #7 - Stale Term Rejection**
Ensure that candidates with stale terms cannot win elections. If a candidate's term is less than a voter's current term, the vote should be rejected.

**Test #8 - Leader Heartbeat Behavior**
Validate that an elected leader sends periodic heartbeats (AppendEntries RPCs with no entries) to all followers to maintain authority and prevent new elections.

### Log Replication Tests

**Test #9 - Basic Log Entry Replication**
Verify that when a client sends a command to the leader, it is appended to the leader's log, replicated to a majority of followers, and committed.

**Test #10 - Log Consistency Across Followers**
Confirm that after replication, all nodes have identical logs for all committed entries. The logs should match in both term and index for all entries up to the commit index.

**Test #11 - Entry Commitment Rules**
Validate that entries are only committed when they have been replicated to a majority of servers and that the leader only considers entries from its current term committed.

**Test #12 - AppendEntries Consistency Check**
Test that followers reject AppendEntries RPCs that don't match their log at the previous log index and term, forcing the leader to decrement nextIndex and retry.

**Test #13 - Leader Append-Only Property**
Confirm that a leader never overwrites or deletes entries in its own log. It can only append new entries.

**Test #14 - Log Matching Property**
Verify that if two logs contain an entry with the same index and term, the logs are identical in all entries up to that index.

**Test #15 - Follower Crash During Replication**
Simulate a follower crashing mid-replication. When it recovers, verify that the leader successfully replicates any missing entries.

**Test #16 - Network Partition During Replication**
Create a partition that isolates a minority of followers. Verify that the leader continues to commit entries with the majority, and upon healing, the isolated nodes catch up.

### Safety and Persistence Tests

**Test #17 - Leader Completeness Property**
Validate the election restriction: a candidate must have all committed entries in its log to win an election. Verify that candidates without the latest committed entries cannot be elected.

**Test #18 - State Persistence After Reboot**
Test that nodes persist their current term, voted for, and log to stable storage. After restart, nodes should restore this state and not vote for stale candidates.

**Test #19 - No Committed Entry Loss**
Verify that once an entry is committed, it remains in all future leaders' logs. Simulate multiple leader failures and ensure no committed entry is lost.

**Test #20 - Read-Only Operations with Stale Leader**
Test that read-only operations processed by leaders without checking up-to-dateness might return stale data. Verify implementation of read index or lease read mechanisms.

**Test #21 - Cluster Safety During Leader Crash**
Crash the leader immediately after committing an entry but before responding to client. Verify that the new leader contains that committed entry and no inconsistency occurs.

### Membership Changes Tests

**Test #22 - Single Server Addition**
Test the process of adding one new server to a 3-node cluster, resulting in a 4-node cluster. Verify that the cluster continues to operate during and after the transition.

**Test #23 - Single Server Removal**
Remove one server from a 5-node cluster, resulting in a 4-node cluster. Verify that the cluster maintains consensus and no disruption occurs.

**Test #24 - Joint Consensus Configuration**
During membership changes, verify that the cluster operates in joint consensus where both old and new configurations can make decisions.

**Test #25 - Concurrent Configuration Changes**
Test handling of overlapping membership changes. Verify that only one configuration change can be in progress at a time.

### Snapshot and Log Compaction Tests

**Test #26 - Snapshot Creation**
Verify that when the log exceeds a threshold, the leader can create a snapshot of the current state and truncate the log.

**Test #27 - InstallSnapshot RPC**
Test that a slow follower or new server can receive and apply a snapshot from the leader to catch up quickly.

**Test #28 - Snapshot and Log Consistency**
After installing a snapshot, verify that the node's log and state machine remain consistent with the rest of the cluster.

### Fault Tolerance and Recovery Tests

**Test #29 - Minority Failure Tolerance**
Kill two nodes in a 5-node cluster. Verify that the remaining three nodes can continue to elect a leader and process requests.

**Test #30 - Majority Failure Recovery**
Kill three nodes in a 5-node cluster. Verify that the remaining two cannot elect a leader or commit entries. When nodes recover, verify that the cluster returns to normal operation.

**Test #31 - Leader Step-Down on Higher Term**
When a leader receives an RPC with a higher term, it should step down to follower immediately.

**Test #32 - Network Delay Tolerance**
Introduce varying network delays between nodes. Verify that the algorithm remains correct despite delayed messages.

**Test #33 - Duplicate RPC Handling**
Test that nodes correctly handle duplicate RPCs (AppendEntries and RequestVote) without corrupting state.

**Test #34 - Out-of-Order Message Handling**
Verify that the system correctly handles messages that arrive out of order due to network conditions.

### Edge Cases and Specific Scenarios

**Test #35 - Previous Leader Confusion**
Scenario where a partitioned leader believes it's still leader while a new leader is elected in the majority partition. Upon reconnection, verify the old leader steps down.

**Test #36 - Disconnected Follower Catch-up**
Take a follower offline for an extended period, allowing the log to grow significantly. Upon reconnection, verify it catches up efficiently.

**Test #37 - Zero-Entry Commitment**
Test that leaders commit a no-op entry at the start of their term to commit any entries from previous terms.

**Test #38 - Client Request Timeout**
Simulate client requests timing out. Verify that the leader retries until the operation succeeds or is determined to be impossible.

**Test #39 - Concurrent Client Requests**
Flood the leader with concurrent client requests. Verify that requests are processed in order and consistency is maintained.

**Test #40 - Long-Running Stability**
Run the cluster under load for an extended period, performing random node failures/recoveries. Verify that consistency invariants hold throughout.

### Performance and Scalability Tests

**Test #41 - Throughput Under Load**
Measure the number of committed entries per second under various load conditions and cluster sizes.

**Test #42 - Recovery Time After Leader Failure**
Measure the time from leader failure to new leader election under different network conditions.

**Test #43 - Log Replication Latency**
Measure the time from client request to commitment under varying cluster sizes and network delays.

## Expected Results and Validation

Each test should validate specific Raft properties:
- **Election Safety**: At most one leader can be elected in a given term
- **Leader Append-Only**: Leaders never overwrite their logs
- **Log Matching**: If two logs contain an entry with same index/term, they're identical up to that index
- **State Machine Safety**: If a server applies a log entry at a given index, no other server applies a different entry at that index
- **Leader Completeness**: If a log entry is committed, that entry will be present in the logs of all future leaders
