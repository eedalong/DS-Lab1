
package raft 

import "errors"
import "sync"
import "fmt"
// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
// index is older than the existing snapshot.
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
// snapshot is temporarily unavailable.
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

type MemoryStorage struct {
	// Protects access to all fields. Most methods of MemoryStorage are
	// run on the raft goroutine, but Append() is run on an application
	// goroutine.
	sync.Mutex
	// ents[i] has raft log position i+snapshot.Metadata.Index
	ents []Entry
}

// NewMemoryStorage creates an empty MemoryStorage.
func NewMemoryStorage() MemoryStorage {
	ms := MemoryStorage{
		// When starting from scratch populate the list with a dummy entry at term zero.
		ents: make([]Entry, 1),
	}
	ms.ents[0] = Entry{
		Term:0,
		Index:0,
		Data: 0,
	}
	return ms 
}

// Entries implements the Storage interface.
func (ms *MemoryStorage) Entries(lo, hi int) ([]Entry, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if lo < offset {
		fmt.Println("LOGS ARE COMPACTED FOR START", lo)
		return nil, ErrCompacted
	}
	if hi > ms.lastIndex()+1 {
		fmt.Printf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
		return nil, ErrCompacted
	}
	// only contains dummy entries.
	/*
	if len(ms.ents) == 1 {
		return nil, ErrUnavailable
	}
	*/

	ents := ms.ents[lo-offset : hi-offset]
	return ents, nil
}
// Term implements the Storage interface.
func (ms *MemoryStorage) Term(i int) (int, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	// 说明要获取的log已经被从缓存中删除掉了
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}

// LastIndex implements the Storage interface.
func (ms *MemoryStorage) LastIndex() (int, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(), nil
}

func (ms *MemoryStorage) lastIndex() int {
	return ms.ents[0].Index + int(len(ms.ents)) - 1
}

// FirstIndex implements the Storage interface.
func (ms *MemoryStorage) FirstIndex() (int, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(), nil
}

func (ms *MemoryStorage) firstIndex() int {
	return ms.ents[0].Index + 1
}

// ONLY DELETE LOGS FOLLOWING THE LOG THAT DOESNT MATCH 
func (ms *MemoryStorage) Append(ents []Entry)(bool, error){

	last_index, _ := ms.LastIndex()
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	index :=0
	for index =0; index < len(ents); index++{
		if ents[index].Index > last_index ||ms.ents[ents[index].Index].Term != ents[index].Term{
		
			ms.ents = append(ms.ents[:ents[index].Index - offset], ents[index:]...)
			break
		}
	}
	return true, nil 
}
func(ms *MemoryStorage) Delete(entry Entry){
	ms.Lock()
	defer ms.Unlock()
	ms.ents =  ms.ents[: len(ms.ents)-1]
	return
}

func(ms *MemoryStorage) InitEnts(ents []Entry){
	ms.Lock()
	defer ms.Unlock()
	ms.ents = append(make([]Entry,0),ents...)
}

