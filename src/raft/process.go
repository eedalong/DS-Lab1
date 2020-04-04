
package raft 
type Progress struct {
	Match, Next int

	RecentActive int 


}
// ProgressMap is a map of *Progress.
type ProgressMap map[int]*Progress



type ProgressTracker struct {

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
			Next: 0,
		}
	}
	return ProgressTracker{
		Progresses: Progresses,
		Votes: Votes,
	}
}

func (tracker *ProgressTracker) Update(index int, vote bool){
	tracker.Votes[index] = vote
}
func (tracker *ProgressTracker) Granted() int {
	granted := 0
	for _, v := range(tracker.Votes){
		if v{
			granted ++ 
		}
	}
	return granted

}
func (tracker *ProgressTracker) ClearVotes(peers int){
	index := 0
	for index =0; index < peers; index++ {
		tracker.Votes[index] = false
	}
}
func (tracker *ProgressTracker) Active(instanceId int, active bool){
	if active{
		tracker.Progresses[instanceId].RecentActive = 0
	}else{
		tracker.Progresses[instanceId].RecentActive += 1
	}
}
func (tracker *ProgressTracker) CheckQuroum( quroum int) bool {
	connected := 0
	for _,v := range(tracker.Progresses){
		if v.RecentActive <= 1{
			connected ++
		}
	}
	return 2 * (connected+1) > quroum
}
