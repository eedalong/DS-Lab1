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
package raft 

import "math/rand"
import "fmt"
import "math"
import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
// import "bytes"
// import "../labgob"

// None is a placeholder node ID used when there is no leader.
const None int = -1
const noLimit = math.MaxUint64
type StateType int 

const (
	StateFollower = 0
	StateCandidate = 1
	StateLeader    = 2
)
var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
	"StatePreCandidate",
}

type Raft struct {
        mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	dead      int32               // set by Kill()

        id int 

	Term int 
	vote int 
	state int 

	// the log
	raftLog *RaftLog

	tracker ProgressTracker


	msgs []Message

	// the leader id
	lead  int 

	checkQuorum bool
	preVote     bool
	votedNum    int 	
	heartbeatElapsed int 
	electionElapsed int 
	heartbeatTimeout int
	electionTimeout  int
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int


}

func (rf * Raft) becomeLeader(){
	fmt.Println("INSTANCE ", rf.id, " BECOME LEADER`")
	
	// update info of myself 
	rf.state = StateLeader
	rf.lead = rf.id
	
	// broadCast info
	index := 0 
	log_index, log_term := rf.raftLog.Lastlog()
	for index=0; index < len(rf.peers); index++{
		if index == rf.id {
			continue 
		}
		heartbeat := Message{
			From: rf.id,
			To: index,
			Type:MsgHeartbeat,
			Term: rf.Term,
			LogTerm: log_term,
			Index: log_index,
		}
		reply :=  &Message{} 
		go rf.send(heartbeat, reply)
	} 
}

func (rf *Raft) Heartbeat(args *Message) *Message{
	//fmt.Println("INSTANCE ", rf.id, rf.Term, "RECEIVE HEARTBEAT FROM", args.From, args.Term)
	reply := &Message{
		From: rf.id,
		Term: rf.Term,

	}
	if rf.Term > args.Term{
		return reply 
	}
	if (rf.lead == None || rf.lead == args.From){
		//fmt.Println("INSTANCE ", rf.id, "BECOME FOLLOWER OF ", args.From)
		rf.becomeFollower(args.Term, args.From)
		return reply 
	}
	if args.Term > rf.Term && rf.state == StateLeader{
		//fmt.Println("INSTANCE ", rf.id, "BECOME FOLLOWER OF ", args.From)
		rf.becomeFollower(args.Term, args.From)
		return reply 

	}
	return reply 

}
func (rf *Raft) kickHeartbeat() error {
	if rf.state != StateLeader{
		return nil
	}
	rf.mu.Lock()
	rf.heartbeatElapsed ++
	index := 0
	log_index, log_term := rf.raftLog.Lastlog()
	rf.mu.Unlock()
	if rf.heartbeatElapsed >= rf.heartbeatTimeout{
		rf.heartbeatElapsed = 0
		for index = 0; index < len(rf.peers); index ++ {
			if index == rf.id{
				continue 
			}
			heartbeatMessage := Message{
				From: rf.id,
				To: index,
				Type: MsgHeartbeat,
				Term: rf.Term,
				Index: log_index,
				LogTerm: log_term,
			}


			go rf.send(heartbeatMessage, nil)
		}
	}
	return nil 

}
func (rf * Raft) becomeCandidate() error{
	
	rf.mu.Lock()	
	//fmt.Println("INSTANCE ", rf.id, "BECOME CANDIDATE")
	rf.lead = None
	rf.state = StateCandidate
	rf.electionElapsed = 0
	rf.electionTimeout = rand.Intn(50) + 100
	rf.Term ++
	rf.vote = rf.id
	rf.tracker.ClearVotes(len(rf.peers))
	rf.tracker.Update(rf.id, true)
	rf.mu.Unlock()
	return nil 

}
func (rf  *Raft)kickElection() error {
	if rf.state == StateLeader{
		return  nil 
	}
	rf.mu.Lock()
	rf.electionElapsed ++
	rf.mu.Unlock()
	index := 0 
	if rf.electionElapsed >= rf.electionTimeout{
		

		rf.becomeCandidate()
		//fmt.Println("INSTANCE ", rf.id, "START ELECTION")

		logIndex, logTerm := rf.raftLog.Lastlog()
		for index = 0; index < len(rf.peers); index++{
			if index == rf.id{
				continue 
			}
			voteMessage := Message{
				From: rf.id,
				To: index,
				Type: MsgVote,
				Term: rf.Term,
				Index: logIndex,
				LogTerm: logTerm,
			}


			go rf.send(voteMessage, nil)
		}
	}
	return nil 

}

func (rf *Raft) Tick(){
	for {
		// sleep for a unit time 
		time.Sleep(10 * time.Millisecond)
		rf.kickHeartbeat()
		rf.kickElection()
		if rf.killed(){
			break
		}

	}

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
        term = rf.Term
        isleader = (rf.lead == rf.id)
	return term, isleader
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


// example RequestVote RPC handler.
//
func (rf *Raft) MayVote(args *Message) *Message{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log_index, log_term := rf.raftLog.Lastlog()
	reply := &Message{
		Type: MsgVoteResp,
		From: rf.id,
		To: args.From,
		Term: rf.Term,
		LogTerm: log_term,
		Index: log_index,
	}
	if rf.state == StateLeader{
		reply.Reject = true
		return reply 
	}
	// when leader is alive, ignore the request
	if rf.lead != None && rf.electionElapsed < rf.electionTimeout{
		
		reply.Reject = true
		return reply 
	}
	
	// Your code here (2A, 2B).
        if args.Term < rf.Term || rf.vote != None{
		//fmt.Println("INSTANCE ",rf.id,"REJECT ", args.From, "BECAUSE ALREADY VOTED", "DETAILS: ",args.Term, " ", rf.Term, " ", rf.vote, rf.lead)	
		reply.Reject = true
		return reply 
	}
	if !rf.raftLog.MoreUpdate(args.Index, args.LogTerm){	
		
		//fmt.Println("INSTANCE ",rf.id,"REJECT ", args.From, "BECAUSE STALE DATA")	
		reply.Reject = true 
		return reply 
	}
	reply.Reject = false
	rf.vote = args.From
	//fmt.Println("INSTANCE ", rf.id, "VOTE FOR ", args.From)
	return reply

}
func (rf *Raft) RequestVote(args *Message, reply *Message) {
	if args.Type == MsgVote{
		reply = rf.MayVote(args)
		return
	}
	if args.Type == MsgHeartbeat{
		reply = rf.Heartbeat(args)
		return 
	}
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
func (rf *Raft) send(args Message, reply *Message ) bool {
        if args.Type == MsgHeartbeat{
		reply = &Message{}
		//fmt.Println("INSTANCE", rf.id, "SENDING HEARTBEAT TO", args.To)
		ok := rf.peers[args.To].Call("Raft.RequestVote", &args, reply)
		//fmt.Println("CHECK NETWORK STATUS FOR ", rf.id, "FROM", reply.From, "IS ", ok)
		rf.mu.Lock()
		rf.tracker.Active(reply.From, ok)
		if !rf.tracker.CheckQuroum(len(rf.peers)) {
			rf.mu.Unlock()
			//fmt.Println("INSTANCE ", rf.id, "BACK OFF FROM LEADER")
			rf.becomeFollower(rf.Term, None)	
			return ok
		}
		rf.mu.Unlock()
		if ok && reply.Term > rf.Term{
			rf.becomeFollower(reply.Term, None)
		}
		
		return ok 
	} else if  args.Type == MsgVote{
		reply = &Message{}
		ok := rf.peers[args.To].Call("Raft.RequestVote", &args, reply)
		rf.mu.Lock()
		// check vote result
		if ok{
			
			rf.tracker.Update(reply.From, !reply.Reject)	
		}
		// update Term 
		granted := rf.tracker.Granted()
		// check if i win most votes 
		if rf.state != StateLeader && 2 * granted > len(rf.peers){
			//fmt.Println("INSTANCE ", rf.id, "BECOME LEDER" )
			rf.becomeLeader()
		}
		rf.mu.Unlock()
		// we become leader if we accept more than half votes
		return ok 
	}
	return true
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
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) becomeFollower(Term int, Lead int){
	rf.mu.Lock()
	rf.Term = Term
	rf.lead = Lead
	rf.vote = None 
	rf.tracker.ClearVotes(len(rf.peers))
	rf.state = StateFollower
	rf.electionElapsed = 0
	rf.electionTimeout = rand.Intn(50) + 100
	rf.mu.Unlock()
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
	rf.id = me
	rf.vote = None 
	rf.heartbeatTimeout = 10
	rand.Seed(time.Now().UnixNano())
	rf.raftLog = NewLog()
	rf.tracker = NewTracker(len(peers))
	// Your initialization code here (2A, 2B, 2C)
        rf.becomeFollower(1, None)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.Tick()

	return rf
}
