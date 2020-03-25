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

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER

	HEART_FREQUENCE = 100
)

// 通过 applyCh 返回一个已提交日志的消息
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

type Entry struct {
	Term    int32
	Command interface{}
}

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
	state         int32
	currentTerm   int32
	voteFor       int
	electionTimer *time.Timer
	voteCh        chan struct{}
	appendCh      chan struct{}
	applyCh       chan ApplyMsg

	voteAcquried int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
	logs       []Entry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = int(rf.currentTerm)
	isleader = rf.isState(LEADER)
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {

	// Your data here (2A, 2B).
	Term        int32
	CandidateID int

	LastLogIndex int
	LastLogTerm  int32
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	CurrentTerm int32
	VoteOrNot   bool
}

//
// example RequestVote RPC handler.
// 这是一个处理投票请求的RPC
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	if args.Term == rf.currentTerm {
		if rf.voteFor == -1 {
			if rf.logs[len(rf.logs)-1].Term < args.LastLogTerm ||
				(rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && len(rf.logs)-1 <= args.LastLogIndex) {
				reply.VoteOrNot = true
				rf.voteFor = args.CandidateID
			}

		} else {
			reply.VoteOrNot = false
		}

	} else if args.Term < rf.currentTerm {
		reply.VoteOrNot = false
		reply.CurrentTerm = rf.currentTerm
	} else {
		rf.currentTerm = args.Term
		rf.uptoState(FOLLOWER)

		rf.voteFor = -1
		DPrintf("term:%v server:%v state:%v", rf.currentTerm, rf.me, rf.state)
		if rf.voteFor == -1 {
			if rf.logs[len(rf.logs)-1].Term < args.LastLogTerm ||
				(rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && len(rf.logs)-1 <= args.LastLogIndex) {
				reply.VoteOrNot = true
				rf.voteFor = args.CandidateID
			}

		} else {
			reply.VoteOrNot = false
		}

	}

	reply.CurrentTerm = rf.currentTerm

	rf.mu.Unlock()
	if reply.VoteOrNot == true {
		go func() { rf.voteCh <- struct{}{} }()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should ( 把结果写回到reply )
// pass &reply.

//下面说args 和 reply 传递给Call()应该和handler 函数里面声明的类型一样？啥意思？
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//

// Call()应该是在labrpc里面定义了。
// return false or true; 如果在规定的timeout内返回了，那么它会返回true，否则是false
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//

// Call() 函数保证了一定会返回，除非handler函数没有返回。所以没有必要自己去实现Call()是延迟。
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//

// look at the comments in ../labrpc/labrpc.go for more details.
// 注意，传递给RPC的那些字段应该都是大写字母开头的，传递结构体的，应该是它的指针，而不是它本身
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

type AppendEntryArgs struct {
	Term         int32
	LeaderID     int
	PrevLogIndex int
	PreLogTerm   int32

	Entries      []Entry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term        int32
	AppendOrNot bool
}

//一个发送投票请求的例子？
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) applyLogs() {
	//DPrintf("server: %v  term: %v  state: %v, commited: %v  logs:%v", rf.me, rf.currentTerm, rf.state, rf.commitIndex, rf.logs)
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		entry := rf.logs[rf.lastApplied]
		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- msg
	}

}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	//fmt.Println("args.En: ", args.Entries)
	rf.mu.Lock()
	//defer rf.mu.Unlock()
	flag := true
	if args.Term < rf.currentTerm {
		reply.AppendOrNot = false
		reply.Term = rf.currentTerm
		flag = false
	} else if args.Term == rf.currentTerm {

		if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PreLogTerm {

			reply.Term = rf.currentTerm
			reply.AppendOrNot = false

		} else {
			//fmt.Println("args.En: ", args.Entries)
			//var i int
			//length := len(rf.logs)

			if args.PrevLogIndex+len(args.Entries) > len(rf.logs)-1 {
				rf.logs = append(rf.logs[0:args.PrevLogIndex+1], args.Entries...)
			} else {
				for i := args.PrevLogIndex + 1; i-args.PrevLogIndex-1 < len(args.Entries); i++ {
					j := i - args.PrevLogIndex - 1
					if args.Entries[i-args.PrevLogIndex-1].Term != rf.logs[i].Term {
						rf.logs = append(rf.logs[0:i], args.Entries[j:]...)
						break
					}
				}
			}

			/*
				if len(rf.logs) > args.PrevLogIndex+len(args.Entries) {
					length = args.PrevLogIndex + len(args.Entries)
					for i = args.PrevLogIndex; i < length; i++ {
						if rf.logs[i].Term != args.Entries[i-args.PrevLogIndex].Term {
							//fmt.Println(rf.logs[0:i], args.Entries[i-args.PrevLogIndex:])
							rf.logs = append(rf.logs[0:i], args.Entries[i-args.PrevLogIndex:]...)
							break
						}
					}
				} else {
					rf.logs = append(rf.logs[0:args.PrevLogIndex+1], args.Entries...)
				}
			*/

			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = args.LeaderCommit
				if tIndex := len(rf.logs) - 1; args.LeaderCommit > tIndex {
					rf.commitIndex = tIndex
				}
				rf.applyLogs()
			}

			reply.AppendOrNot = true
			reply.Term = rf.currentTerm
		}

	} else {
		reply.Term = rf.currentTerm

		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.uptoState(FOLLOWER)

		reply.AppendOrNot = false

		DPrintf("term:%v server:%v state:%v", rf.currentTerm, rf.me, rf.state)

	}
	//reply.Term = rf.currentTerm
	//fmt.Println("server: ", rf.me, " term : ", rf.currentTerm, rf.logs, "state: ", rf.state)
	//DPrintf("server: %v  term: %v  logs:%v state: %v, commited: %v", rf.me, rf.currentTerm, rf.logs, rf.state, rf.commitIndex)
	rf.mu.Unlock()

	if flag {
		go func() {
			rf.appendCh <- struct{}{}
		}()
	}

	return

}

// Start()这是一个raft暴露给外部服务的接口：大概就是处理一条指令，由谁处理之类的，以及如何处理
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully( 优雅的 ).
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := false

	if rf.isState(LEADER) {
		isLeader = true
		//注意append entry必须与index设置在一个加锁位置，如果推迟append，会导致concurrent start失败。
		rf.logs = append(rf.logs, Entry{Term: rf.currentTerm, Command: command})
		index = len(rf.logs) - 1
		term = int(rf.currentTerm)
		rf.persist()
	}

	return index, term, isLeader
}

// tester是不会终止goroutines, 是每个raft测试之后之后创建的，它调用了Kill()
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
// 任何goroutine有一个long-running loop都应该调用killed()来检查它是否应该暂停
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

// service or tester 都会创建一个 Raft server, servers 的 ports 存在 peers[] 中;
// persister是一个地方用来保存持久化状态的地方， 并且也是恢复server最近状态的数据来源，
// applyCh 是一个channel, tester or service 希望 raft 发送 ApplyMsg 消息的地方
// Make()应该要快速返回，所以需要开启一个goroutines处理那些长期的work

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

func randElectionInterval() time.Duration {

	var UP int32 = 300

	var DOWN int32 = 150
	t := rand.New(rand.NewSource(time.Now().UnixNano()))
	interval := t.Int31n(UP-DOWN) + DOWN
	return time.Duration(interval) * time.Millisecond

}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state = FOLLOWER
	rf.electionTimer = time.NewTimer(randElectionInterval()) //设置一个随机的选举时间
	rf.voteFor = -1
	rf.appendCh = make(chan struct{})
	rf.voteCh = make(chan struct{})
	rf.voteAcquried = 0
	rf.logs = make([]Entry, 0)
	rf.logs = append(rf.logs, Entry{Command: 0, Term: 0}) //使得logs的所有相关元素的索引从1开始
	rf.commitIndex = 0
	rf.lastApplied = -1
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startWork()

	return rf
}

func (rf *Raft) startWork() {
	// 根据自身状态选择动作：
	// Leader: 定期发送 AppendEntries RPC

	// Follower: 启动election定时，并处理来自Leader的请求或candidate的请求

	// Candidate: 发起选举过程
	for {

		if rf.killed() {
			break
		}
		state := atomic.LoadInt32(&rf.state)
		switch state {
		case FOLLOWER:
			// 定期监听心跳信息，否则变为CANDIDATE
			select {
			case <-rf.electionTimer.C: //超时，更改状态为CANDIDATE, 重置定时器
				//DPrintf("server: %v  term: %v  logs:%v state: %v, commited: %v", rf.me, rf.currentTerm, rf.logs, rf.state, rf.commitIndex)
				rf.mu.Lock()
				rf.uptoState(CANDIDATE)
				//rf.leaderElection()
				//fmt.Println("FOLLOWER")
				rf.mu.Unlock()
				rf.leaderElection()
			case <-rf.voteCh: //进行投票，需要重置定时器
				//DPrintf("server: %v  term: %v  logs:%v state: %v, commited: %v", rf.me, rf.currentTerm, rf.logs, rf.state, rf.commitIndex)
				rf.electionTimer.Reset(randElectionInterval())
			case <-rf.appendCh: //日志检测，需要重启定时器
				//DPrintf("server: %v  term: %v  logs:%v state: %v, commited: %v", rf.me, rf.currentTerm, rf.logs, rf.state, rf.commitIndex)
				rf.electionTimer.Reset(randElectionInterval())
			}

		case CANDIDATE:
			//fmt.Println("CANDIDATE")
			// 调用 RequestVote RPC, 发起选举过程, 超时需要重新发起选举
			select {
			case <-rf.electionTimer.C:
				//DPrintf("server: %v  term: %v  logs:%v state: %v, commited: %v", rf.me, rf.currentTerm, rf.logs, rf.state, rf.commitIndex)
				//超时重新选举
				//fmt.Println("dddd")
				rf.leaderElection()
			case <-rf.appendCh:
				// 收到来自leader的心跳检测，所以变回follower
				rf.mu.Lock()
				rf.uptoState(FOLLOWER)
				rf.mu.Unlock()
			default:
				// 统计票数，判断自己是否是leader
				rf.mu.Lock()
				if rf.voteAcquried > len(rf.peers)/2 {

					rf.uptoState(LEADER)
					n := len(rf.peers)
					rf.nextIndex = make([]int, n) // 初始化为leader的logs的最后一个index+1
					for index, _ := range rf.nextIndex {
						rf.nextIndex[index] = len(rf.logs)
					}
					rf.matchIndex = make([]int, n) //初始化为0

				}
				rf.mu.Unlock()
			}

		case LEADER:

			//DPrintf("server: %v  term: %v  logs:%v state: %v, commited: %v", rf.me, rf.currentTerm, rf.logs, rf.state, rf.commitIndex)
			//fmt.Println("LEADER")
			// 定期调用 AppendEntries RPC,广播心跳检测
			//rf.mu.Lock()
			n := len(rf.peers)
			//rf.mu.Unlock()

			for i := 0; i < n; i++ {
				if i == rf.me {
					continue
				}
				if !rf.isState(LEADER) {
					break
				}
				go rf.broadCastAppend(i)
			}
			rf.mu.Lock()
			rf.changeCommit()
			if rf.lastApplied < rf.commitIndex {
				rf.applyLogs()
			}
			rf.mu.Unlock()
			if rf.isState(LEADER) {
				time.Sleep(time.Duration(HEART_FREQUENCE) * time.Millisecond)
			}

		}

	}
}
func (rf *Raft) isState(state int32) bool {

	flag := state == atomic.LoadInt32(&rf.state)
	return flag
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()

	atomic.AddInt32(&rf.currentTerm, 1)
	DPrintf("term:%v server:%v state:%v", rf.currentTerm, rf.me, rf.state)

	rf.electionTimer.Reset(randElectionInterval())
	//fmt.Println("wwww ")

	//fmt.Println("jjj")
	rf.voteAcquried = 1
	rf.voteFor = rf.me

	n := len(rf.peers)

	args := RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.me, LastLogIndex: len(rf.logs) - 1, LastLogTerm: rf.logs[len(rf.logs)-1].Term}
	//fmt.Println("yyyy")
	rf.mu.Unlock()

	for i := 0; i < n; i++ {
		if !rf.isState(CANDIDATE) {
			break
		}
		if i == rf.me {
			continue
		}
		//并行的发送选举消息给其他节点
		go func(node_num int) {
			var replyMsg RequestVoteReply

			if rf.isState(CANDIDATE) && rf.sendRequestVote(node_num, &args, &replyMsg) {
				rf.mu.Lock()
				// 在这个函数执行完之后释放锁，防止处理来自不同节点的消息而导致并发问题

				if replyMsg.VoteOrNot {
					rf.voteAcquried += 1
				} else {

					if replyMsg.CurrentTerm > rf.currentTerm {
						rf.uptoState(FOLLOWER)
						rf.voteFor = -1
						rf.voteAcquried = 0
						rf.currentTerm = replyMsg.CurrentTerm
						DPrintf("term:%v server:%v state:%v", rf.currentTerm, rf.me, rf.state)

					}
				}

				rf.mu.Unlock()
			}

		}(i)
	}

}
func (rf *Raft) uptoState(newState int32) {
	if rf.isState(newState) {
		return
	}
	//nowState := rf.state

	switch newState {
	case FOLLOWER:
		rf.state = FOLLOWER
		rf.voteFor = -1
	case CANDIDATE:
		rf.state = CANDIDATE
		//rf.leaderElection()

	case LEADER:
		rf.state = LEADER
	default:

	}
}
func (rf *Raft) changeCommit() {
	n := len(rf.peers)
	for i := len(rf.logs) - 1; i > rf.commitIndex; i-- {
		cnt := 1
		for j := 0; j < n; j++ {
			if j == rf.me {
				continue
			}
			if rf.matchIndex[j] >= i {
				cnt++
			}
		}
		if cnt > n/2 && rf.logs[i].Term == rf.currentTerm {
			rf.commitIndex = i
			break
		}
	}
}
func (rf *Raft) broadCastAppend(server int) { // 表示把logs的index+1项之后的logs发送给follower

	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//length := len(rf.logs)
	//var index int = length - 1
	//var entry []Entry
	//var index int
	/*
		if index = rf.nextIndex[server] - 1; index+1 <= len(rf.logs)-1 {
			entry = rf.logs[index+1:]
		}
	*/
	//for {
	if rf.killed() {
		return
	}
	if !rf.isState(LEADER) {
		return
	}

	rf.mu.Lock()
	var reply AppendEntryReply

	index := rf.nextIndex[server] - 1
	//fmt.Print("index: ", index)
	var entry []Entry
	if index+1 < len(rf.logs) {
		entry = rf.logs[index+1:]
	}

	args := AppendEntryArgs{Term: rf.currentTerm, LeaderID: rf.me, PrevLogIndex: index,
		PreLogTerm: rf.logs[index].Term, Entries: entry, LeaderCommit: rf.commitIndex}

	rf.mu.Unlock()

	if rf.sendAppendEntry(server, &args, &reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			DPrintf("term:%v server:%v state:%v", rf.currentTerm, rf.me, rf.state)
			rf.currentTerm = reply.Term
			rf.uptoState(FOLLOWER)
			rf.mu.Unlock()
			return
		} else if !reply.AppendOrNot {
			// leader 需要通过返回的term,调整发送的日志，再次发出去
			/*
				for ; index >= 1; index-- {
					if rf.logs[index].Term != reply.Term {
						break
					}
				}
			*/
			if rf.nextIndex[server] > 1 {
				rf.nextIndex[server]--
			}
			rf.mu.Unlock()

		} else if reply.AppendOrNot {

			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1

			//这里需要更新leader 的 commitIndex

			rf.mu.Unlock()
			return
		}
		//rf.mu.Unlock()
	}
	//time.Sleep(10 * time.Millisecond)
	//}

}
