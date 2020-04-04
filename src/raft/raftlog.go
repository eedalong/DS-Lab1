package raft

type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage MemoryStorage


	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64
	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64


}

func NewLog() *RaftLog{
	return &RaftLog{
		committed: 0,
		applied: 0,
		storage: NewMemoryStorage(), 
	}

}
func (rflog *RaftLog) Lastlog()(int, int){
	last_index, _ := rflog.storage.LastIndex()
	last_term, _ := rflog.storage.Term(last_index)
	return last_index, last_term
	
}
func (rflog *RaftLog) MoreUpdate(log_index int, log_term int) bool{
	my_index, my_term := rflog.Lastlog()
	res :=  log_term > my_term ||(log_term == my_term && log_index >= my_index)
	return res 
} 
