
package raft 
import "sync"
//import "fmt"
type Progress struct {
	Match, Next int

	RecentActive int 


}
// ProgressMap is a map of *Progress.
type ProgressMap map[int]*Progress



type ProgressTracker struct {

	mu sync.Mutex
	Progresses ProgressMap

	Votes map[int]bool

}

func NewTracker(peers int) ProgressTracker{
	Votes := make(map[int]bool)
	Progresses := make(map[int]*Progress)
	index :=0
	for index = 0; index < peers; index++{
		Votes[index] = false
		Progresses[index] = &Progress{
			RecentActive: 0,
			Match: 0,
			Next: 1,
		}
	}
	return ProgressTracker{
		Progresses: Progresses,
		Votes: Votes,
	}
}

func (tracker *ProgressTracker) Update(index int, vote bool){
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	tracker.Votes[index] = vote
}
func (tracker *ProgressTracker) Granted() int {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	granted := 0
	for _, v := range(tracker.Votes){
		if v{
			granted ++ 
		}
	}
	return granted

}
func (tracker *ProgressTracker) ClearVotes(peers int){
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	index := 0
	for index =0; index < peers; index++ {
		tracker.Votes[index] = false
	}
}
func (tracker *ProgressTracker) Active(instanceId int, active bool){
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if active{
		tracker.Progresses[instanceId].RecentActive = 0
	}else{
		tracker.Progresses[instanceId].RecentActive += 1
	}
}
func (tracker *ProgressTracker) CheckQuroum( quroum int) bool {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	connected := 0
	for _,v := range(tracker.Progresses){
		if v.RecentActive <= 1{
			connected ++
		}
	}
	return 2 * (connected+1) > quroum
}
func (tracker *ProgressTracker) DecrNext(follower int,val int) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	tracker.Progresses[follower].Next -= val
	if tracker.Progresses[follower].Next < 1{
		tracker.Progresses[follower].Next = 1
	}
	return 	
	
}
func (tracker *ProgressTracker) SetState(follower int, match int, next int){
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	//fmt.Println("UPDATE STATE FOR FOLLOWER", follower, match, next)
	tracker.Progresses[follower].Match = match
	tracker.Progresses[follower].Next = next
	return 
	
}
func (tracker *ProgressTracker) CanCommit(quroum int , leader int, leaderCommit int, Term int) bool {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	success := 0
	for k,v :=range(tracker.Progresses){
		if k == leader{
			continue 
		}
		if v.Match >= leaderCommit{
			success ++
		}
	}
	return 2*(success + 1) > quroum
}
func (tracker *ProgressTracker) State(follower_index int)(int, int){
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.Progresses[follower_index].Match, tracker.Progresses[follower_index].Next
}
