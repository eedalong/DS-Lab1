package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//
import "time"
import "os"
import "strconv"
import "math/rand"
//
// example to show how to declare the arguments
// and reply for an RPC.
//

type PingMessage struct {
	Worker_id string
	Task_type string
	Task_files []string
	Result_files []string
}

type PongMessage struct {
	Worker_id string 
	Reducer_count int
	Task_type string 
	Task_files []string
	Exit bool 	
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}


var defaultLetters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// RandomString returns a random string with a fixed length
func RandomString(n int) string {
	var letters []rune
	rand.Seed(int64(time.Now().Nanosecond()))
	letters = defaultLetters

	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}
