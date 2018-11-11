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
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

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

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

const (
	NONE = -1
)

type LogEntry struct {
	term    int
	command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (Lab1, Lab2, Challenge1).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state            int32
	totalServerCount int
	currentTerm      int
	voteCount        int
	votedFor         int
	log              []*LogEntry
	electionTimer    *time.Timer
}

func (rf *Raft) printRaft() {
	fmt.Printf("[@@@@@ Raft Info @@@@@@]\n")
	fmt.Printf("me: %d\n", rf.me)
	fmt.Printf("state: %d\n", rf.state)
	fmt.Printf("totalServerCount: %d\n", rf.totalServerCount)
	fmt.Printf("currentTerm: %d\n", rf.currentTerm)
	fmt.Printf("voteCount: %d\n\n", rf.voteCount)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (Lab1).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (Challenge1).
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
	// Your code here (Challenge1).
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (Lab1, Lab2).
	Term        int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (Lab1).
	Term        int
	VoteGranted bool
}

// locked by rf.mu
func voteGranted(rf *Raft, args *RequestVoteArgs) bool {
	case1 := args.Term < rf.currentTerm
	case2 := rf.votedFor != NONE && rf.votedFor != args.CandidateId
	return !case1 && !case2
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (Lab1, Lab2).

	rf.mu.Lock()
	fmt.Printf("[Receiver side RequestVote() handler]\n")

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteCount = 0
		rf.state = FOLLOWER
		electionTimeout := getRandomElectionTimeout()
		rf.electionTimer.Reset(time.Duration(electionTimeout) * time.Millisecond)
	}

	//rf.printRaft()
	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted(rf, args)
	if reply.VoteGranted {
		rf.votedFor = args.CandidateId
	}
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	// Your data here (Lab1, Lab2).
	Term     int
	LeaderId int
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (Lab1, Lab2).
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) AppendEntries(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (Lab1, Lab2).
}

//
// example code to send a AppendEntries RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendAppendEntries(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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

	// Your code here (Lab2).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// Your initialization code here (Lab1, Lab2, Challenge1).
	rf.state = FOLLOWER
	rf.totalServerCount = len(rf.peers)
	rf.currentTerm = 0
	rf.voteCount = 0
	rf.votedFor = NONE
	rf.log = make([]*LogEntry, rf.totalServerCount)

	rf.printRaft()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go startLeaderElection(rf)

	return rf
}

func getRandomElectionTimeout() int {
	randomSource := rand.NewSource(time.Now().UnixNano())
	random := rand.New(randomSource)
	electionTimeout := 300 + random.Intn(150)
	return electionTimeout
}

func broadcastRequestVotes(rf *Raft) {
	for i := 0; i < rf.totalServerCount; i++ {
		if i != rf.me {
			args := new(RequestVoteArgs)
			reply := new(RequestVoteReply)
			rf.mu.Lock()
			//fmt.Printf("		[broadcastRequestVotes]\n")
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			rf.mu.Unlock()
			ok := rf.sendRequestVote(i, args, reply)
			if ok && reply.VoteGranted {
				rf.mu.Lock()
				fmt.Printf("	[sendRequestVote returns true, rf.state: %d]\n", rf.state)
				if reply.Term > rf.currentTerm {
					// convert to follower
					rf.currentTerm = reply.Term
					rf.voteCount = 0
					rf.votedFor = NONE
					rf.state = FOLLOWER
					rf.mu.Unlock()
					break
				} else {

					rf.voteCount++
					failureCount := rf.totalServerCount - rf.voteCount
					majorityHolds := 2*failureCount+1 <= rf.totalServerCount
					if majorityHolds {
						// convert to leader
						fmt.Printf("I am a leader[%d]\n", rf.me)
						rf.state = LEADER
						rf.voteCount = 0
						rf.mu.Unlock()
						break
					}
				}

			}

		}
	}
}

func startLeaderElection(rf *Raft) {
	for {
		rf.mu.Lock()
		if rf.state == FOLLOWER || rf.state == CANDIDATE {
			electionTimeout := getRandomElectionTimeout()
			// if rf.state == FOLLOWER, it is the initial case that
			rf.electionTimer = time.NewTimer(time.Duration(electionTimeout) * time.Millisecond)
			rf.mu.Unlock()

			select {
			case <-rf.electionTimer.C:
				rf.mu.Lock()
				//fmt.Printf("state: %d\n", rf.state)
				rf.currentTerm += 1
				rf.state = CANDIDATE
				rf.votedFor = rf.me
				rf.voteCount += 1
				rf.mu.Unlock()

				// send RequestVote RPCs to all other servers
				go broadcastRequestVotes(rf)
				// case <-rf.leaderElected :
				//case <- rf.receivedHeartBeat:
			}

		} else if rf.state == LEADER {

		} else {
			rf.Kill()
		}

	}

}
