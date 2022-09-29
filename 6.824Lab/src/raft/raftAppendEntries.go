package raft

import (
	// "fmt"
	"time"
)

func (rf *Raft) ResetAppendEntryTimers() {
	for i := 0; i < rf.numServer; i++ {
		if i == rf.me {
			continue
		}
		rf.AppendEntriesTimers[i].Stop()
		rf.AppendEntriesTimers[i].Reset(0) // 立即开始发送
	}
}

func (rf *Raft) ResetAppendEntryTimer(server int) {
	rf.AppendEntriesTimers[server].Stop()
	rf.AppendEntriesTimers[server].Reset(HeartBeatTimeout)
}

// GetLastLogTermIndex返回全局的lastLogTerm和lastLogIndex
func (rf *Raft) GetLastLogTermIndex() (int, int) {
	lastLogTerm := rf.logs[len(rf.logs)-1].LogTerm
	lastLogIndex := rf.lastSnapShotIndex + len(rf.logs) - 1
	return lastLogTerm, lastLogIndex
}

// GetPrevLogTermIndex返回全局的prevLogTerm和prevLogIndex
func (rf *Raft) GetPrevLogTermIndex(server int) (int, int) {
	prevLogTerm := rf.logs[rf.matchIndex[server]].LogTerm
	prevLogIndex := rf.logs[rf.matchIndex[server]].LogIndex
	return prevLogTerm, prevLogIndex
}

func (rf *Raft) IsCommitLogs() {
	hasCommited := false // 如果有需要commit的log，就设为true

	// if rf.role == leader {
	// 	fmt.Printf("IsCommitLogs, leader's commit index: %d, lastapplied index: %d\n", rf.commitIndex, rf.lastApplied)
	// 	fmt.Printf("IsCommitLogs, leader's matchIndex[server]: %d, %d, %d\n", rf.matchIndex[0], rf.matchIndex[1], rf.matchIndex[2])
	// }
	for logIndex := rf.commitIndex + 1; logIndex <= len(rf.logs)-1; logIndex++ {
		syncNum := 0
		for server := 0; server < rf.numServer; server++ {
			if rf.matchIndex[server] >= logIndex {
				syncNum++
			}
			if syncNum > rf.numServer/2 {
				rf.commitIndex = logIndex
				hasCommited = true
				break
			}
		}
		// 由于commitlog是连续的，如果出现一个没有达到commit标准的log，那么直接结束
		if rf.commitIndex != logIndex {
			break
		}
	}
	if hasCommited && rf.logs[rf.commitIndex].LogTerm != rf.currentTerm {
		return
	}
	if hasCommited {
		rf.CommitChan <- struct{}{}
	}
}

func (rf *Raft) StartCommit() {
	// fmt.Printf("peer %d commit index: %d, lastapplied index: %d\n", rf.me, rf.commitIndex, rf.lastApplied)
	// fmt.Printf("peer %d matchindex: %d, %d, %d\n", rf.me, rf.matchIndex[0], rf.matchIndex[1], rf.matchIndex[2])

	// msgs := make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)

	if rf.commitIndex > rf.lastApplied {
		// fmt.Printf("peer %d start commit\n", rf.me)
		lastSnapShotIndex := rf.lastSnapShotIndex
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msg := ApplyMsg{
				CommandValid: true,
				// 被snapshot打断后rf.logs改变了，因此需要减去rf.lastSnapShotIndex才能获取原来的log
				Command: rf.logs[i+lastSnapShotIndex-rf.lastSnapShotIndex].Cmd,
				// CommandIndex属于全局index，被打断后i依然线性增长，但是原来的rf.lastSnapShotIndex却变了，导致突增，因此应该保持恒定
				CommandIndex: i + lastSnapShotIndex,
			}
			rf.ApplyMsgChan <- msg
			// 被打断后rf.lastApplied发生了突变，而i保持原来的线性增长，因此这里不可以让i直接对它赋值
			rf.lastApplied++ // 这里传给service的index是log在rf.logs中的位置，不是真正的全局index
			// fmt.Printf("peer %d commit log %d\n", rf.me, i+lastSnapShotIndex)
		}
		// fmt.Printf("peer %d commit index: %d, lastapplied index: %d\n", rf.me, rf.commitIndex, rf.lastApplied)

	}
}

func (rf *Raft) GetEntry(server int) (AppendEntries, bool) {
	_, lastLogIndex := rf.GetLastLogTermIndex()
	// fmt.Printf("get entry, peer %d matchIndex: %d\n", server, rf.matchIndex[server])
	// fmt.Printf("leader %d matchindex: %d, %d, %d\n", rf.me, rf.matchIndex[0], rf.matchIndex[1], rf.matchIndex[2])
	if rf.matchIndex[server] > len(rf.logs)-1 {
		rf.matchIndex[server] = len(rf.logs) - 1
	}
	if rf.logs[rf.matchIndex[server]].LogIndex == lastLogIndex { // no log need to send
		// println("a heartbeat")
		args := AppendEntries{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex + rf.lastSnapShotIndex,
			IsHeartBeat:  true,
		}
		args.PrevLogTerm, args.PrevLogIndex = rf.GetPrevLogTermIndex(server)
		return args, true
	}
	// fmt.Printf("for peer %d has new log, matchIndex[server] = %d, lastLogIndex = %d\n", server, rf.matchIndex[server], lastLogIndex)
	args := AppendEntries{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex + rf.lastSnapShotIndex,
		IsHeartBeat:  false,
	}
	entries := append([]Log{}, rf.logs[rf.matchIndex[server]+1:]...)
	args.Entries = entries
	args.PrevLogTerm, args.PrevLogIndex = rf.GetPrevLogTermIndex(server)
	return args, false
}

func (rf *Raft) RequestEntry(args *AppendEntries, reply *AppendEntriesReply) {
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	snapIndex := rf.lastSnapShotIndex
	rf.ChangeRole(follower)
	rf.ResetElectionTimer()
	LastLogTerm, LastLogIndex := rf.GetLastLogTermIndex()
	reply.PeerTerm = rf.currentTerm
	reply.PeerLastLogTerm = LastLogTerm
	reply.PeerLastLogIndex = LastLogIndex
	reply.AppendSuccess = false
	reply.MatchIndex = rf.lastSnapShotIndex
	if args.Term < rf.currentTerm {
		return
	}

	/*
		append失败的两个原因：
		1. index没对上，但是term对上了，找index重新写，小改动；
		2. term没对上，找term重新写，大改动，需要重新写整个term
	*/
	// 首先需要考虑一下follower的lastSnapShotIndex
	if args.PrevLogIndex < rf.lastSnapShotIndex {
		reply.AppendSuccess = false
		reply.MatchIndex = rf.lastSnapShotIndex
		return
	} else if args.PrevLogIndex == rf.lastSnapShotIndex {
		if !args.IsHeartBeat {
			rf.logs = append(rf.logs[0:args.PrevLogIndex+1-snapIndex], args.Entries...)
			rf.persist()
			// fmt.Printf("peer %d's logs: %v\n", rf.me, rf.logs)
		}
		rf.commitIndex = args.LeaderCommit - rf.lastSnapShotIndex
		rf.CommitChan <- struct{}{}
		reply.AppendSuccess = true
		reply.MatchIndex = rf.lastSnapShotIndex + len(args.Entries)

		return
	}
	if args.PrevLogIndex > LastLogIndex {
		// 属于index对不上的问题
		// 这里是最明显一个的错误，不管是不是同一个term，中间必然存在空块，
		// 需要将LastLogIndex作为matchIndex，补上更早的log
		reply.AppendSuccess = false
		reply.MatchIndex = LastLogIndex
		return
	}
	if args.PrevLogTerm == rf.logs[args.PrevLogIndex-snapIndex].LogTerm {
		// 最正常的情况，leader找到follower与自己配对的log，
		// 然后把这个log后面的所有日志全部接上自己的，可能有覆盖也可能没有
		if !args.IsHeartBeat {
			// 如果args包含有要写入的内容，就在这里写入
			rf.logs = append(rf.logs[0:args.PrevLogIndex+1-snapIndex], args.Entries...)
			rf.persist()
			// fmt.Printf("peer %d's logs: %v\n", rf.me, rf.logs)
		}
		rf.commitIndex = args.LeaderCommit - rf.lastSnapShotIndex
		rf.CommitChan <- struct{}{}
		reply.AppendSuccess = true
		reply.MatchIndex = args.PrevLogIndex + len(args.Entries)

		return
	}

	if args.PrevLogTerm == rf.logs[args.PrevLogIndex-snapIndex].LogTerm {
		// args.PrevLog和follower对应位置的log在term上匹配商量，但是idx匹配不上，
		// 那就找到follower的上一个term的最后一个index作为matchIndex，
		// 把现在这个term全部重新写一遍
		idx := args.PrevLogIndex - rf.lastSnapShotIndex // idx是局部index，指在rf.logs中的相对位置
		for idx >= rf.commitIndex && rf.logs[idx].LogTerm == rf.currentTerm {
			idx--
		}
		reply.AppendSuccess = false
		reply.MatchIndex = idx + rf.lastSnapShotIndex
	} else {
		// 如果连term都匹配不上，直接从0开始，全部重写
		reply.AppendSuccess = false
		reply.MatchIndex = rf.lastSnapShotIndex
		// // 如果args.PrevLogTerm != LastLogTerm，
		// // 当然它也不会满足args.PrevLogTerm == rf.logs[args.PrevLogIndex].LogTerm
		// // 那么配对的term一定乱了，那就重新找term，从lastLogTerm一直往前找，
		// // 直到找到follower的一个term < args.prevLogTerm，
		// // 这个term如果存在一个小于args.prevIndex的log，那它就是要返回的matchIndex，
		// // leader在这个index的log[index].LogTerm会等于term，从这里开始覆盖follower的log
		// for index := LastLogIndex; index >= rf.commitIndex; index-- {
		// 	if rf.logs[index].LogTerm < args.PrevLogTerm && index < args.PrevLogIndex {
		// 		// fmt.Printf("peer %d append false, case6\n", rf.me)
		// 		reply.AppendSuccess = false
		// 		reply.MatchIndex = index
		// 		break
		// 	}
		// }
	}
}

func (rf *Raft) AppendEntriesToPeer(server int) {
	if rf.role != leader {
		rf.ResetAppendEntryTimer(server) // 只有leader才能发送entry
		return
	}
	RTCTimer := time.NewTimer(RPCTimeout)
	chOk := make(chan bool)

	for {
		args, isheartBeat := rf.GetEntry(server)
		reply := AppendEntriesReply{}
		rf.ResetAppendEntryTimer(server)
		go func() {
			// if !args.IsHeartBeat {
			// 	fmt.Printf("leader %d send entry to peer %d\n", rf.me, server)
			// }
			ok := rf.peers[server].Call("Raft.RequestEntry", &args, &reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			chOk <- ok
		}()

		select {
		case <-rf.EndChan:
			return
		case <-RTCTimer.C:
			// fmt.Printf("leader %d send entry to %d timeout\n", rf.me, server)
			return
		case ok := <-chOk:
			if !ok {
				// fmt.Printf("leader %d send entry to %d failed\n", rf.me, server)
				continue
			}
		}

		if reply.PeerTerm > rf.currentTerm { // 存在问题，leader下台
			// fmt.Printf("leader %d term < follower %d term\n", rf.me, server)
			// rf.currentTerm = reply.PeerTerm // 不应该更新落后leader，防止落后leader在选举中获胜
			rf.ChangeRole(follower)
			rf.ResetElectionTimer()
			return
		}
		// if heartbeat { // 心跳不参与appendEntries
		// 	return
		// }

		if reply.AppendSuccess {
			rf.matchIndex[server] = reply.MatchIndex - rf.lastSnapShotIndex
			if !isheartBeat {
				// fmt.Printf("leader %d send entry to %d success\n", rf.me, server)
				// fmt.Printf("leader %d's logs: %v\n", rf.me, rf.logs)
				rf.IsCommitLogs()
			}
			// _, lastLogIndex := rf.GetLastLogTermIndex()
			// fmt.Printf("rf.matchIndex[server]: %d, lastLogIndex:%d\n", rf.matchIndex[server], lastLogIndex)
			// 检测commit

			return
		} else {
			rf.matchIndex[server] = reply.MatchIndex - rf.lastSnapShotIndex
			if rf.matchIndex[server] < 0 { // 需要append的follower部分，已经被leader保存为快照了
				rf.matchIndex[server] = 0
				rf.InstallSnapshotChan[server] <- struct{}{}
				// time.Sleep(5 * time.Millisecond)
				return
			}
			RTCTimer.Stop()
			RTCTimer.Reset(RPCTimeout)
			continue
		}
	}

}
