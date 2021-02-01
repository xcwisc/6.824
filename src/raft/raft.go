package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// enum of 3 roles in candiate election
const (
	Leader    = "Leader"
	Follower  = "Follower"
	Candidate = "Candidate"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentRole   string    // the current role of this server
	currentTerm   int       // the latest term the server has seen
	voteFor       int       // candidateid for current term
	lastHeartbeat time.Time // the last time the server hear a heartbeat from the leader
	// log
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm := rf.currentTerm
	isleader := rf.currentRole == Leader
	return currentTerm, isleader
}

func (rf *Raft) GetFullState() {
	DPrintf("me: %v, currentRole: %v, currentTerm: %v, voteFor: %v",
		rf.me, rf.currentRole, rf.currentTerm, rf.voteFor)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// convertToCandidate convert the current server to candidate and start an election
func (rf *Raft) convertToCandidate() {
	cond := sync.NewCond(&rf.mu)
	grantedVotes := 1
	totalVotes := 1

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentRole = Candidate
	rf.currentTerm++
	rf.voteFor = rf.me
	startTerm := rf.currentTerm

	// send RequestVote RPC calls to other servers to inform them the start of election
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		args := &RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
		}
		reply := &RequestVoteReply{}
		go func(i int) {
			rf.sendRequestVote(i, args, reply)
			// at this point, call to RequestVote is successful and this thread is not holding a lock
			// we should then process the reply
			rf.mu.Lock()
			defer rf.mu.Unlock()

			VoteGranted := reply.VoteGranted
			totalVotes++
			if VoteGranted {
				grantedVotes++
			} else {
				term := reply.Term
				if rf.currentTerm < term {
					rf.currentTerm = term
					rf.currentRole = Follower
				}
			}
			cond.Broadcast()
		}(i)
	}

	// wait until all the votes are collected
	for grantedVotes*2 < len(rf.peers) && totalVotes != len(rf.peers) {
		cond.Wait()
	}

	if grantedVotes*2 > len(rf.peers) && rf.currentRole == Candidate && startTerm == rf.currentTerm {
		// this server should be the new leader
		rf.convertToLeader()
	}
}

// convertToLeader convert the current server to Leader and send initial heartbeat
func (rf *Raft) convertToLeader() {
	rf.currentRole = Leader
	rf.sendHeartBeats()
}

//
// RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

//
//  RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}

	if args.Term != rf.currentTerm || rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		reply.VoteGranted = true
		rf.lastHeartbeat = time.Now()
		rf.voteFor = args.CandidateId
		if rf.currentTerm < args.Term {
			rf.currentRole = Follower
			rf.currentTerm = args.Term
		}
		return
	}
	reply.VoteGranted = false
	return
}

// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	for ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok; {
		// keep retrying to send the RPC unless
		// the current term is larger than the term in args (other candidate has got the vote or elapse time run out)
		rf.mu.Lock()
		if rf.currentTerm > args.Term {
			reply.VoteGranted = false
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

//
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

//
//  AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentRole = Follower
		rf.currentTerm = args.Term
	}

	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		term := reply.Term
		if term > rf.currentTerm {
			rf.currentTerm = term
			rf.currentRole = Follower
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) randElectionTimeoutGen(min, max int) (get func(currentTerm int) time.Duration) {
	var timeout time.Duration
	savedTerm := rf.currentTerm
	rand.Seed(time.Now().UnixNano())
	timeout = time.Duration(rand.Intn(max-min+1)+min) * time.Millisecond

	get = func(currentTerm int) time.Duration {
		if currentTerm == savedTerm {
			return timeout
		}

		rand.Seed(time.Now().UnixNano())
		timeout = time.Duration(rand.Intn(max-min+1)+min) * time.Millisecond
		savedTerm = currentTerm
		return timeout
	}
	return
}

// this function periodically check if the leader has not
// send a heartbeat or granting vote to candidate
func (rf *Raft) checkElectionTimeout() {
	RandElectionTimeoutGen := rf.randElectionTimeoutGen(500, 750)
	for {
		rf.mu.Lock()
		if rf.currentRole == Follower || rf.currentRole == Candidate {
			lastHeartbeat := rf.lastHeartbeat
			elapsed := time.Since(lastHeartbeat)
			electionTimeout := RandElectionTimeoutGen(rf.currentTerm)

			if elapsed > electionTimeout {
				// this means the current term is over due to slight randomized electionTimeout
				// now this server should start an election
				rf.lastHeartbeat = time.Now()
				go rf.convertToCandidate()
			}
		}
		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}

// this function be periodically called to send heartbeats if the current server is a leader
// note that this method is not thread safe, so locking is required to call this function
func (rf *Raft) sendHeartBeats() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		reply := &AppendEntriesReply{}
		go rf.sendAppendEntries(i, args, reply)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentRole = Follower
	rf.currentTerm = 0
	rf.lastHeartbeat = time.Now()
	rf.voteFor = -1

	// Your initialization code here (2A, 2B, 2C).

	// this goroutine is responsible for checking if leader is dead (haven't sent a heartbeat longer than ElectionTimeout)
	go rf.checkElectionTimeout()

	// this goroutine is reponsible for sending heartbeats if the current server is a leader
	go func(rf *Raft) {
		for {
			rf.mu.Lock()
			if rf.currentRole == Leader {
				rf.sendHeartBeats()
			}
			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
