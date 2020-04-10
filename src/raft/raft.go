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
import "bytes"
import "../labgob"

// None is a placeholder node ID used when there is no leader.
const None int = -1
const noLimit = math.MaxUint64
// locks 
var persist_lock sync.Mutex
var vote_lock sync.Mutex
var start_lock sync.Mutex
var request_lock sync.Mutex
var leader_update_lock sync.Mutex
var follower_update_lock sync.Mutex
var append_lock sync.Mutex
var heartbeat_lock sync.Mutex
//var sync_lock sync.Mutex
var SyncCalled int 
var HeartbeatCalled int 
var VoteCalled int
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
	heartbeatElapsed int 
	electionElapsed int 
	heartbeatTimeout int
	electionTimeout  int
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int
	applyChan chan ApplyMsg

}

func (rf * Raft) becomeLeader(){
	//fmt.Println("INSTANCE ", rf.id, " BECOME LEADER WITH COMMIT", rf.raftLog.Commit())
	rf.mu.Lock()
	// update info of myself 
	rf.state = StateLeader
	rf.lead = rf.id
	rf.heartbeatElapsed = 8
	last_index := rf.raftLog.LastIndex()
	rf.tracker.ResetAll(len(rf.peers), last_index)
	rf.mu.Unlock()
}

func (rf *Raft) FollowerApply(){
	follower_update_lock.Lock()
	defer follower_update_lock.Unlock()
	rf.mu.Lock()
	if rf.state != StateFollower{
		rf.mu.Unlock()
		return 
	}
	rf.mu.Unlock()
	committed := rf.raftLog.Commit()
	applied := rf.raftLog.Apply()
	for ;applied <= committed; applied ++ {
		ents, _ := rf.raftLog.Logs(applied, applied+1)
		rf.raftLog.SetApply(applied)
		rf.applyChan <- ApplyMsg{
			CommandValid: true,
			Command: ents[0].Data,
			CommandIndex: ents[0].Index,
		}
		//fmt.Println("FOLLOWER", rf.id, "APPLIED DATA", ents)
	}
	rf.raftLog.SetApply(committed + 1)

}
func (rf *Raft) Heartbeat(args *Message, reply *Message){
	//fmt.Println("INSTANCE ", rf.id, rf.Term, "RECEIVE HEARTBEAT FROM", args.From, args.Term)
	heartbeat_lock.Lock()
	defer heartbeat_lock.Unlock()
	rf.mu.Lock()
	reply.From = rf.id
	reply.Term = rf.Term
	reply.Reject = true
	if rf.Term > args.Term{
		rf.mu.Unlock()
		return 
	}
	
	// BECOME FOLLOWER IF REQUEST CONTAINS A LARGER TERM 
	if args.Term > rf.Term{
		rf.mu.Unlock()
		rf.becomeFollower(args.Term, None)
		rf.mu.Lock()
	}

	if (rf.lead == None || rf.lead == args.From){
		//rf.mu.Lock()
		//fmt.Println("INSTANCE ", rf.id, "BECOME FOLLOWER OF ", args.From)
		
		if rf.raftLog.Term(args.PrevIndex)== args.PrevTerm{
			reply.Reject = false
			if args.Commit > rf.raftLog.Commit(){
				
				rf.raftLog.SetCommit(rf.min(args.Commit, args.PrevIndex))
			}
			rf.mu.Unlock()	
			
			rf.becomeFollower(args.Term, args.From)
			return 
		}
		rf.mu.Unlock()
		
		rf.becomeFollower(args.Term, args.From)
		reply.Reject = true
		
		return 
	}
	rf.mu.Unlock()
	return 

}
func (rf *Raft) kickHeartbeat() error {
	rf.mu.Lock()
	if rf.state != StateLeader{
		rf.mu.Unlock()
		return nil
	}
	rf.heartbeatElapsed ++
	index := 0
	log_index, log_term := rf.raftLog.Lastlog()
	committed := rf.raftLog.Commit()
	rf.mu.Unlock()
	if rf.heartbeatElapsed >= rf.heartbeatTimeout{
		rf.heartbeatElapsed = 0
		for index = 0; index < len(rf.peers); index ++ {
			if index == rf.id{
				continue 
			}
			_, next := rf.tracker.State(index)
		
			ents, _:= rf.raftLog.Logs(next-1, next)
		        ents_send := append(make([]Entry, 0), ents...)
			rf.mu.Lock()	
			heartbeatMessage := Message{
				From: rf.id,
				To: index,
				Type: MsgHeartbeat,
				Term: rf.Term,
				Index: log_index,
				LogTerm: log_term,
				Commit: committed,
				PrevTerm: ents_send[0].Term,
				PrevIndex: ents_send[0].Index,
			}
			rf.mu.Unlock()
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
	rf.electionTimeout = rand.Intn(10) + 30
	rf.Term ++
	rf.vote = None
	rf.tracker.ClearVotes(len(rf.peers))
	//rf.tracker.Update(rf.id, true)
	rf.mu.Unlock()
	
	return nil 

}
func (rf  *Raft)kickElection() error {
	
	// critical area
	rf.mu.Lock()
	if rf.state == StateLeader{
		rf.mu.Unlock()
		return  nil 
	}
	rf.electionElapsed ++
	rf.mu.Unlock()

	index := 0 
	if rf.electionElapsed >= rf.electionTimeout{
		

		rf.becomeCandidate()
		//fmt.Println("INSTANCE ", rf.id, "START ELECTION")

		for index = 0; index < len(rf.peers); index++{
			if index == rf.id{
				continue 
			}
			
			logIndex, logTerm := rf.raftLog.Lastlog()
			
			// critical area 	
			rf.mu.Lock()
			voteMessage := Message{
				From: rf.id,
				To: index,
				Type: MsgVote,
				Term: rf.Term,
				Index: logIndex,
				LogTerm: logTerm,
			}
			rf.mu.Unlock()


			go rf.send(voteMessage, nil)
		}
	}
	return nil 

}

func (rf *Raft) Tick(){
	count := 0
	for {
		// sleep for a unit time 
		time.Sleep(time.Millisecond)
		if count == 0 {
			go rf.kickHeartbeat()
			go rf.kickElection()
			if rf.state == StateLeader{	
				go rf.UpdateLeader()
				rf.SyncCommand(-1)
			}
			if rf.state == StateFollower{
				go rf.FollowerApply()
			}
		}
	        	
		count =  (count + 1)%10
		go rf.persist()
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
	 rf.mu.Lock()
	 defer rf.mu.Unlock() 
	 length := rf.raftLog.LastIndex() + 1
	 ents, _:= rf.raftLog.Logs(0, length)
	 committed := rf.raftLog.Commit()
	 applied := rf.raftLog.Apply()

	 w := new(bytes.Buffer)
	 e := labgob.NewEncoder(w)

	 e.Encode(rf.Term)
	 e.Encode(rf.vote)
	 e.Encode(committed)
	 e.Encode(applied)

	 e.Encode(length)
	 
	 e.Encode(ents)
	 
	 data := w.Bytes()
	 //fmt.Println("INSTACE", rf.id, "PERSIST DATA", rf.Term, rf.vote, ents)
	 rf.persister.SaveRaftState(data)
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
	 rf.mu.Lock()
	 defer rf.mu.Unlock()
	 r := bytes.NewBuffer(data)
	 d := labgob.NewDecoder(r)
	 var term int
	 var vote int
	 var length int 
	 var ents []Entry
	 var committed, applied int 
	 if d.Decode(&term) != nil ||d.Decode(&vote) != nil ||d.Decode(&committed) != nil||d.Decode(&applied) != nil || d.Decode(&length)!= nil {
	   fmt.Println("DECODE ERROR")
	 } else {
             		
	     rf.Term = term
	     rf.vote = vote
	     rf.raftLog.SetApply(applied)
	     rf.raftLog.SetCommit(committed)
	     ents =  make([]Entry, length)
	     d.Decode(&ents)
		
	     //fmt.Println("INSTANCE", rf.id, "READ PERSIST DATA", term ,vote, ents)	
	     rf.raftLog.InitEnts(ents)
	}
}


// example RequestVote RPC handler.
//
func (rf *Raft) MayVote(args *Message, reply *Message){
	// RESET VOTE AND TERM IF COMMING REQUEST CONTAINS LARGER TERM
	vote_lock.Lock()
	defer vote_lock.Unlock()
	rf.mu.Lock()
	if args.Term > rf.Term{
		rf.mu.Unlock()
		rf.becomeFollower(args.Term, None)
		rf.mu.Lock()
	}
	defer rf.mu.Unlock()
	reply.Type = MsgVoteResp
	reply.From = rf.id
	reply.Reject = true
	reply.Term = rf.Term


	if args.Term < rf.Term{
		return 
	}

	if rf.vote == args.From{
		reply.Reject = false
		return 
	}
	// when leader is alive, ignore the request
	if rf.lead != None && rf.electionElapsed < rf.electionTimeout{
		
	//	fmt.Println("INSTANCE ",rf.id,"REJECT ", args.From, "BECAUSE LEADER IS ALIVE", "DETAILS: ",args.Term, " ", rf.Term, " ", rf.vote, rf.lead)	
		reply.Reject = true
		return 
	}
	
	// Your code here (2A, 2B).
        if rf.vote != None{
		//fmt.Println("INSTANCE ",rf.id,"REJECT ", args.From, "BECAUSE ALREADY VOTED", "DETAILS: ",args.Term, " ", rf.Term, " ", rf.vote, rf.lead)	
		reply.Reject = true
		return 
	}
	if !rf.raftLog.MoreUpdate(args.Index, args.LogTerm){	
	
		//fmt.Println("INSTANCE ",rf.id,"REJECT ", args.From, "BECAUSE STALE DATA", args.Index, args.LogTerm)	
		reply.Reject = true

		return 
	}
	// GRANT A VOTE 
	reply.Reject = false
	rf.electionElapsed = 0
	rf.vote = args.From
	//fmt.Println("INSTANCE ", rf.id, "VOTE FOR ", args.From, "DETAIL", rf.Term,args.Term)
	
}
func (rf *Raft) min(val1, val2 int)int {
	if val1 > val2{
		return val2
	}
	return val1
}

func (rf *Raft) AppendEntries(args* Message, reply *Message){
	append_lock.Lock()
	defer append_lock.Unlock()
	rf.mu.Lock()

	reply.From = rf.id
	reply.To = args.From
	reply.Term = rf.Term
	reply.Reject = true
	if args.Term < rf.Term{
		reply.Reject = true
		rf.mu.Unlock()
		return  
	}
	/*
	if args.Term > rf.Term{
		reply.Reject = true
		rf.mu.Unlock()
		return 
	}
	*/
	rf.mu.Unlock()

	rf.becomeFollower(args.Term, args.From)
	appendRes := rf.raftLog.MayAppend(args.PrevIndex, args.PrevTerm, args.Entries)
	if appendRes && args.Commit > rf.raftLog.Commit(){

		rf.raftLog.SetCommit(rf.min(args.Commit, args.Entries[len(args.Entries)-1].Index))
	}
	if appendRes{
		rf.persist()	
		reply.Reject = false
		
		//fmt.Println("FOLLOWER", rf.id, " APPEND FROM LEADER", rf.lead, args.From, args.Entries)
	}else{
		conflict_index, conflict_term := rf.raftLog.FindConflict(args.PrevTerm, args.PrevIndex)
		reply.ConflictIndex = conflict_index
		reply.ConflictTerm = conflict_term	
	}
	return 
}


func (rf *Raft) RequestVote(args *Message, reply *Message) {
	request_lock.Lock()
	defer request_lock.Unlock()
	reply.TermSent = args.Term
	if args.Type == MsgVote{
		rf.MayVote(args, reply)
		return
	}
	if args.Type == MsgHeartbeat{

		rf.Heartbeat(args, reply)
		return 
	}
	if args.Type == MsgApp{
		rf.AppendEntries(args, reply)
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
		
		//fmt.Println(rf.id, "WITH TERM", rf.Term, "SEND HEARTBEAT TO", args.To)
		ok := rf.peers[args.To].Call("Raft.RequestVote", &args, reply)
		
			
		rf.tracker.Active(reply.From, ok)
		rf.mu.Lock()
		if ok && reply.TermSent != args.Term{
			return ok 
		}
		/*
		if rf.state == StateLeader && !rf.tracker.CheckQuroum(len(rf.peers)) {
			fmt.Println("INSTANCE ", rf.id, "BACK OFF FROM LEADER")
			rf.mu.Unlock()
			rf.becomeFollower(reply.Term, None)	
			return ok
		}
		*/
		if ok && rf.state ==  StateLeader && reply.Term > rf.Term{
			
			//fmt.Println("INSTANCE ", rf.id, "BACK OFF FROM LEADER", reply.Term ,rf.Term)
			rf.mu.Unlock()
			rf.becomeFollower(reply.Term, None)
			return ok 
		}
		rf.mu.Unlock()
		return ok 
	} else if  args.Type == MsgVote{
		reply = &Message{}
		ok := rf.peers[args.To].Call("Raft.RequestVote", &args, reply)
		
		rf.mu.Lock()
		if ok && reply.TermSent != rf.Term{
			return ok 
		}	
		if reply.Term > rf.Term{
			rf.mu.Unlock()
			rf.becomeFollower(reply.Term, None)
			return ok
		}
		if ok && reply.Term == rf.Term{
			if rf.state != StateCandidate{
				rf.mu.Unlock()
				return ok  
			}
			rf.tracker.Update(reply.From, !reply.Reject)	
			// update Term 
			granted := rf.tracker.Granted()
		
			// check if i win most votes 
			if 2 * granted > len(rf.peers) || (rf.vote == None && 2*(granted +1)> len(rf.peers)){
				if rf.vote == None{
					rf.vote = rf.id
				}
				rf.mu.Unlock()
				rf.becomeLeader()
				return ok 
			}
		}
		rf.mu.Unlock()
		return ok
 
	}else if args.Type == MsgApp{
		reply = &Message{}
		
		ok := rf.peers[args.To].Call("Raft.RequestVote", &args, reply)
		rf.mu.Lock()
		
		if ok && reply.TermSent != rf.Term{

			return ok 
		}	
	
		if ok && rf.state == StateLeader && reply.Term > rf.Term{
			
			//fmt.Println("INSTANCE ", rf.id, "BACK OFF FROM LEADER", reply.Term ,rf.Term)
			rf.mu.Unlock()
			rf.becomeFollower(reply.Term, None)
			return ok
		}
		if ok && rf.state == StateLeader && reply.Term == rf.Term{
			// check if success
			rf.mu.Unlock()
			rf.UpdateFollower(&args, reply)
			return ok
		}
		rf.mu.Unlock()
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
	start_lock.Lock()
	defer start_lock.Unlock()
	rf.mu.Lock()
	index := -1
	term := rf.Term
	isLeader := true
	// check if i am the leader
	if rf.state != StateLeader{
		rf.mu.Unlock()
		return index, term, false 
	}
	// append log to its own log buffer
	log_index :=rf.raftLog.Append(command, rf.Term)
	
	// start Sync
	rf.mu.Unlock()
	rf.persist()	
	return log_index, term, isLeader
}
func (rf *Raft) UpdateLeader(){
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	leader_update_lock.Lock()
	defer leader_update_lock.Unlock()
	rf.mu.Lock()
	if rf.state != StateLeader{
		rf.mu.Unlock()
		return 
	}
	committed:= rf.raftLog.Commit()
	if committed == 0{
		committed ++
	}

	for {
	
		//fmt.Println("CHECK COMMIT", committed)	
		if committed > rf.raftLog.LastIndex(){
			break
		}
		if committed > 0 && !rf.tracker.CanCommit(len(rf.peers), rf.id, committed, rf.Term){

			break
		}
		if committed > rf.raftLog.Commit() && rf.Term == rf.raftLog.Term(committed){
			
			rf.raftLog.SetCommit(committed)
		}
		committed ++
	}
	rf.mu.Unlock()
	committed  = rf.raftLog.Commit()
	applied := rf.raftLog.Apply()
	
	for ;applied <= committed; applied ++ {
			ents, _ := rf.raftLog.Logs(applied, applied+1)
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command: ents[0].Data,
				CommandIndex: ents[0].Index,
			}
			//fmt.Println("LEADER APPLIED LOG ", ents[0])
	}
	rf.raftLog.SetApply(committed + 1)

}

func (rf *Raft) UpdateFollower(args *Message, reply *Message){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Reject == true{
	//	fmt.Println("FOLLOWER", reply.From, "REJECT APPEND")
		if rf.state != StateLeader{
			return 
		}
		//if reply.ConflictTerm == None{
		rf.tracker.SetState(reply.From, 0, reply.ConflictIndex)
		//}
		//rf.SyncCommand(reply.From)	
		return
	}

	match, next:= args.Entries[len(args.Entries)-1].Index, args.Entries[len(args.Entries)-1].Index+1
	//fmt.Println("UPDATE MATCH AND NEXT FOR", reply.From, match, next)
	rf.tracker.SetState(reply.From, match, next)
	
	return

}

func (rf *Raft)SyncCommand(follower int){

	SyncCalled ++ 
	index := 0
	rf.mu.Lock()
	if rf.state != StateLeader{
		rf.mu.Unlock()
		return 
	}
	rf.mu.Unlock()
	for index=0;index<len(rf.peers); index++{
		// check for each follower's nextIndex
		if index == rf.id || (follower!=-1 && index!=follower){
			continue
		}
		// check if leader can send append msg
		
		_, next := rf.tracker.State(index)
		last_index, _:= rf.raftLog.Lastlog()
		leaderCommit := rf.raftLog.Commit()
		ents, _:= rf.raftLog.Logs(next-1, last_index +1)
		ents_send := append(make([]Entry,0), ents...)
		// send AppendEntry Message
		//fmt.Println("LEADER", rf.id, "SYNC COMMAND TO", index, next-1, last_index+1)
		rf.mu.Lock()
		message := Message{
			Term: rf.Term,
			Commit: leaderCommit,
			To: index,
			From: rf.id,
			Entries: ents_send[1:],
			Type:MsgApp,
			PrevTerm: ents[0].Term,
			PrevIndex: ents[0].Index,
		}
		rf.mu.Unlock()
		if last_index < next{
			continue 
		}

		go rf.send(message, nil)
	}
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
	rf.electionTimeout = rand.Intn(10) + 30
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
	rf.applyChan  = applyCh	
	rf.tracker = NewTracker(len(peers))
	// Your initialization code here (2A, 2B, 2C)
        rf.becomeFollower(1, None)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	SyncCalled = 0
	VoteCalled = 0
	HeartbeatCalled = 0
	go rf.Tick()

	return rf
}
