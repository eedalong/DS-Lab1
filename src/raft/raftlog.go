package raft

import "sync"
//import "fmt"
type RaftLog struct {
	
	mu sync.Mutex
	// storage contains all stable entries since the last snapshot.
	storage MemoryStorage


	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed int
	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied int


}

func NewLog() *RaftLog{
	return &RaftLog{
		committed: 0,
		applied: 1,
		storage: NewMemoryStorage(), 
	}

}
func (rflog *RaftLog) Lastlog()(int, int){
	last_index, _ := rflog.storage.LastIndex()
	last_term, _ := rflog.storage.Term(last_index)
	return last_index, last_term
	
}
func (rflog *RaftLog) Term(index int) int{
	res, _ := rflog.storage.Term(index)
	return res 
}
func (rflog *RaftLog) LastIndex()int{
	last_index, _ := rflog.storage.LastIndex()
	return last_index
}

func (rflog *RaftLog) MoreUpdate(log_index int, log_term int) bool{
	my_index, my_term := rflog.Lastlog()
	res :=  log_term > my_term ||(log_term == my_term && log_index >= my_index)
	return res 
}
 
func (rflog *RaftLog) Append(command interface{}, Term int) int{
	
	last_index, _ := rflog.storage.LastIndex()
	newIndex := last_index + 1
	entry  := Entry{
		Term: Term,
		Index: newIndex,
		Data: command,
	}
	ents := make([]Entry, 1)
	ents[0] = entry
	rflog.storage.Append(ents)
	return newIndex
}
func (rflog *RaftLog) Logs(lo, hi int)([]Entry, error){
	ents, err := rflog.storage.Entries(lo, hi)
	return ents, err
}
func (rflog *RaftLog) MayAppend(prevIndex int, prevTerm int, ents []Entry) bool {
	// check previous log, fails if doesnt match
	previous_term, _:= rflog.storage.Term(prevIndex)
	if previous_term != prevTerm{
		//logs, _ := rflog.Logs(0, rflog.LastIndex()+1) 
		//fmt.Println("REJECTED ", logs)
		return false
	}
	// append to log buffer
	res, _ := rflog.storage.Append(ents)
	//rflog.SetCommit(ents[len(ents)-1].Index)
	return res


}

func (rflog *RaftLog) IncrCommit(){
	rflog.mu.Lock()
	defer rflog.mu.Unlock()
	rflog.committed ++
	return 
}
func (rflog *RaftLog) SetCommit(val int){
	rflog.mu.Lock()
	defer rflog.mu.Unlock()
	rflog.committed = val
	return 

}

func (rflog *RaftLog) Commit()int{
	rflog.mu.Lock()
	defer rflog.mu.Unlock()
	return rflog.committed

}

func (rflog *RaftLog) Apply()int{
	rflog.mu.Lock()
	defer rflog.mu.Unlock()
	return rflog.applied

}
func (rflog *RaftLog) SetApply(val int){
	rflog.mu.Lock()
	defer rflog.mu.Unlock()
	rflog.applied = val
	return 

}
func (rflog *RaftLog) InitEnts(ents []Entry){
	rflog.storage.InitEnts(ents)
}

func (rflog *RaftLog) FindConflict(term, index int) (int, int){
	if index > rflog.LastIndex(){
		return rflog.LastIndex() +1, None
	}
	conflict_term, _ := rflog.storage.Term(index)
	conflict_index := rflog.storage.FindFirst(conflict_term)
	return conflict_index, conflict_term

}
