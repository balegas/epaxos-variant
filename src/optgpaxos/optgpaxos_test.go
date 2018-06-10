package optgpaxos

import (
    "testing"
    "fastrpc"
    "chansmr"
    "genericsmr"
    "sync"
    "bufio"
    "genericsmrproto"
    "state"
    "math/rand"
    "optgpaxosproto"
    "log"
    "math"
    "fmt"
)


type RepChan struct{
    C *channels
    R *Replica
}

// Cannot write multiple messages to same channel. When unmarshalling, if there are multiple messages in the
// channel, only the first is kept. I am not sure how to make thee buffered reader read a bounded number of bytes.

//Assumes buffer is large enought to get messages.
const BufferSize = 4096
var connections map[int32]*connection
var reps []*RepChan
var leaderIOChan *bufio.ReadWriter
var mutex *sync.Mutex
var control = make(chan bool)

func initReplicas(count int, thrifty bool) []*RepChan{
    ret := make([]*RepChan, count)
    isLeader := true

    connections = make(map[int32]*connection)

    for i := 0 ; i < count; i++ {
        ic := make(chan byte, BufferSize)
        oc := ic
        connections[int32(i)] = &connection{chansmr.NewChanReader(ic), chansmr.NewChanWriter(oc)}
    }

    for i := 0 ; i < count; i++ {
        repiChan := &channels{
            make(chan *genericsmr.Propose, BufferSize),
            make(chan fastrpc.Serializable, BufferSize),
            make(chan fastrpc.Serializable, BufferSize),
            make(chan fastrpc.Serializable, BufferSize),
            make(chan fastrpc.Serializable, BufferSize),
            make(chan fastrpc.Serializable, BufferSize),
            make(chan fastrpc.Serializable, BufferSize),
            make(chan fastrpc.Serializable, BufferSize),
            make(chan fastrpc.Serializable, BufferSize),
            make(chan fastrpc.Serializable, BufferSize),
        }

        ret[i] = &RepChan{
            repiChan,
            NewReplicaStub(i, make([]string, count), isLeader, thrifty, false, false, false, false, repiChan, connections, control),
        }

        isLeader = false
    }
    return ret
}

func setup(replicaCount int, thrifty bool){
    reps = initReplicas(replicaCount, thrifty)
    leaderChan := make(chan byte, BufferSize)
    proposeChanR := bufio.NewReader(chansmr.NewChanReader(leaderChan))
    proposeChanW := bufio.NewWriter(chansmr.NewChanWriter(leaderChan))
    leaderIOChan = bufio.NewReadWriter(proposeChanR, proposeChanW)
    mutex = &sync.Mutex{}
}

func TestInitFastRound(t *testing.T) {
    setup(3, true)
    reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(0, state.PUT, 0, 0), leaderIOChan.Writer, mutex}
    <- control

    fastAccepts := make([]optgpaxosproto.FastAccept, len(reps))
    for i := 1; i < len(reps); i++{
        msg :=  &optgpaxosproto.FastAccept{}
        code := make([]byte,1)
        connections[int32(i)].is.Read(code)
        msg.Unmarshal(connections[int32(i)].is)
    }

    for _, p := range fastAccepts{
        if p.Ballot != 0 {
            t.Errorf("Wrong ballot number.")
        }
        if p.LeaderId!= 0{
            t.Errorf("Wrong leader id.")
        }
    }

}

func TestFirstRound(t *testing.T) {
  setup(3, true)
  reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(0, state.PUT, 0, 0), leaderIOChan.Writer, mutex}
  <- control
  executeFastPath(true)

  reply := new(genericsmrproto.ProposeReplyTS)
  reply.Unmarshal(leaderIOChan.Reader)
  log.Printf("Reply: %v", reply)
  if reply.OK != 1 {
      t.Errorf("Failed Fast Round.")
  }
}

func TestTwoFastRound(t *testing.T) {
   setup(5, true)

   reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(0, state.PUT, 0, 0), leaderIOChan.Writer, mutex}
   <- control

   executeFastPath(true)

   reply := new(genericsmrproto.ProposeReplyTS)
   reply.Unmarshal(leaderIOChan.Reader)
   log.Printf("Reply: %v", reply)
   if reply.OK != 1 {
       t.Errorf("Failed FAST Round.")
   }

   reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(1, state.PUT, 1, 1), leaderIOChan.Writer, mutex}
   <- control

   executeFastPath(true)

   reply = new(genericsmrproto.ProposeReplyTS)
   reply.Unmarshal(leaderIOChan.Reader)
   log.Printf("Reply: %v", reply)
   if reply.OK != 1 {
       t.Errorf("Failed FAST Round.")
   }
}

func TestNFastRound(t *testing.T) {
    setup(7, true)

    for i := 1; i <= 10; i++ {
        reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(int32(i), state.PUT, state.Key(i), int64(i)), leaderIOChan.Writer, mutex}
        <- control

        executeFastPath(true)

        reply := new(genericsmrproto.ProposeReplyTS)
        reply.Unmarshal(leaderIOChan.Reader)
        log.Printf("Reply: %v", reply)
        if reply.OK != 1 {
            t.Errorf("Failed FAST Round.")
        }
    }
}

//func TestRepeatedId(t *testing.T) {
//    setup(3, true)
//
//    reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(0, state.PUT, 0, 0), leaderIOChan.Writer, mutex}
//    <- control
//
//    executeFastPath(true)
//
//    reply := new(genericsmrproto.ProposeReplyTS)
//    reply.Unmarshal(leaderIOChan.Reader)
//    log.Printf("Reply: %v", reply)
//    if reply.OK != 1 {
//        t.Errorf("Failed FAST Round.")
//    }
//
//    reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(0, state.PUT, 0, 0), leaderIOChan.Writer, mutex}
//    <- control
//
//    reply = new(genericsmrproto.ProposeReplyTS)
//    reply.Unmarshal(leaderIOChan.Reader)
//    log.Printf("Reply: %v", reply)
//    if reply.OK != 0 {
//        t.Errorf("Should ignore repeated command")
//    }
//
//}

func TestFastRoundReorderNoConflict(t *testing.T) {
    setup(3, true)
    fQ := int(math.Ceil(3*float64(len(reps))/4))

    reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(0, state.PUT, 0, 0), leaderIOChan.Writer, mutex}
    <- control

    executeFastPath(true)

    reply := new(genericsmrproto.ProposeReplyTS)
    reply.Unmarshal(leaderIOChan.Reader)
    log.Printf("Reply: %v", reply)
    if reply.OK != 1 {
        t.Errorf("Failed Classic Round.")
    }

    reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(1, state.PUT, 1, 1), leaderIOChan.Writer, mutex}
    <- control


    fastAccepts1 := make([]fastrpc.Serializable, fQ)
    for i := 1; i < fQ; i++{
        fastAccepts1[i] = readMessage(int32(i), new(optgpaxosproto.FastAccept))
    }

    reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(2, state.PUT, 2, 2), leaderIOChan.Writer, mutex}
    <- control

    fastAccepts2 := make([]fastrpc.Serializable, fQ)
    for i := 1; i < fQ; i++{
        fastAccepts2[i] = readMessage(int32(i), new(optgpaxosproto.FastAccept))
    }


    //Mix delivery order:
    mixedAcceptReplies1 := make([]fastrpc.Serializable, fQ)
    mixedAcceptReplies1[1] = processMsgAndGetReply(fastAcceptChan(1), fastAccepts1[1], true, 0, new(optgpaxosproto.FastAcceptReply))
    mixedAcceptReplies1[2] = processMsgAndGetReply(fastAcceptChan(2), fastAccepts2[2], true, 0, new(optgpaxosproto.FastAcceptReply))

    for i := 1; i < fQ; i++{
        processMsg(fastAcceptReplyChan(0), mixedAcceptReplies1[i], true)
    }

    mixedFastAcceptReplies2 := make([]fastrpc.Serializable, fQ)
    mixedFastAcceptReplies2[1] = processMsgAndGetReply(fastAcceptChan(1), fastAccepts2[1], true, 0, new(optgpaxosproto.FastAcceptReply))
    mixedFastAcceptReplies2[2] = processMsgAndGetReply(fastAcceptChan(2), fastAccepts1[2], true, 0, new(optgpaxosproto.FastAcceptReply))

    processMsg(fastAcceptReplyChan(0), mixedFastAcceptReplies2[1], true)

    commits1 := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++{
        commits1[i] = readMessage(int32(i), new(optgpaxosproto.Commit))
    }

    for i := 1; i < len(reps); i++{
        processMsg(commitChan(0), commits1[i], true)
    }

    //Cannot buffer two commit messages. Unmarshelling fails.
    processMsg(fastAcceptReplyChan(0), mixedFastAcceptReplies2[2], true)

    commits2 := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++{
        commits2[i] = readMessage(int32(i), new(optgpaxosproto.Commit))
    }

    for i := 1; i < len(reps); i++{
        processMsg(commitChan(int32(i)), commits2[i], true)
    }

    reply = new(genericsmrproto.ProposeReplyTS)
    reply.Unmarshal(leaderIOChan.Reader)
    log.Printf("Reply: %v", reply)
    if reply.OK != 1 && reply.CommandId != 1{
        t.Errorf("Failed Classic Round.")
    }

    reply = new(genericsmrproto.ProposeReplyTS)
    reply.Unmarshal(leaderIOChan.Reader)
    log.Printf("Reply: %v", reply)
    if reply.OK != 1 && reply.CommandId != 2 {
        t.Errorf("Failed Classic Round.")
    }
}

func TestFastRoundReorderConflict(t *testing.T) {
    setup(3, true)
    fQ := int(math.Ceil(3*float64(len(reps))/4))
    q := int(math.Ceil(float64(len(reps))/2))

    reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(0, state.PUT, 0, 0), leaderIOChan.Writer, mutex}
    <- control
    executeFastPath(true)

    reply := new(genericsmrproto.ProposeReplyTS)
    reply.Unmarshal(leaderIOChan.Reader)
    log.Printf("Reply: %v", reply)
    if reply.OK != 1 {
        t.Errorf("Failed Classic Round.")
    }

    reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(1, state.PUT, 1, 1), leaderIOChan.Writer, mutex}
    <- control

    fastAccepts1 := make([]fastrpc.Serializable, fQ)
    for i := 1; i < fQ; i++{
        fastAccepts1[i] = readMessage(int32(i), new(optgpaxosproto.FastAccept))
    }

    reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(2, state.PUT, 1, 2), leaderIOChan.Writer, mutex}
    <- control

    fastAccepts2 := make([]fastrpc.Serializable, fQ)
    for i := 1; i < fQ; i++{
        fastAccepts2[i] = readMessage(int32(i), new(optgpaxosproto.FastAccept))
    }

    //Mix delivery order:
    mixedAcceptReplies1 := make([]fastrpc.Serializable, fQ)
    mixedAcceptReplies1[1] = processMsgAndGetReply(fastAcceptChan(1), fastAccepts1[1], true, 0, new(optgpaxosproto.FastAcceptReply))
    mixedAcceptReplies1[2] = processMsgAndGetReply(fastAcceptChan(2), fastAccepts2[2], true, 0, new(optgpaxosproto.FastAcceptReply))

    for i := 1; i < fQ; i++{
        processMsg(fastAcceptReplyChan(0), mixedAcceptReplies1[i], true)
    }

    accepts1 := make([]fastrpc.Serializable, q)
    for i := 1; i < q; i++{
        accepts1[i] = readMessage(int32(i), new(optgpaxosproto.Accept))
    }

    acceptReplies := make([]fastrpc.Serializable, q)
    for i := 1; i < q; i++{
        acceptReplies[i] = processMsgAndGetReply(acceptChan(int32(i)), accepts1[i], true, 0, new(optgpaxosproto.AcceptReply))
    }

    //Ignores fast accepts after moving to slow phase.
    processMsg(fastAcceptChan(1), fastAccepts1[1], true)
    msg := new(optgpaxosproto.FastAcceptReply)
    code := make([]byte,1)
    connections[0].is.Read(code)
    msg.Unmarshal(connections[0].is)

    if msg.OK != FALSE {
        t.Errorf("Accepted fast accept after moving to slow ballot.")
    }

    for i := 1; i < q; i++{
        processMsg(acceptReplyChan(0), acceptReplies[i], true)
    }

    commits := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++{
        commits[i] = readMessage(int32(i), new(optgpaxosproto.Commit))
    }

    for i := 1; i < len(reps); i++{
        processMsg(commitChan(int32(i)), commits[i], true)
    }

    reps[0].R.dumpDeliveryState()

    // Deliver second pair of reordered messages
    mixedAcceptReplies2 := make([]fastrpc.Serializable, fQ)
    mixedAcceptReplies2[1] = processMsgAndGetReply(fastAcceptChan(1), fastAccepts2[1], true, 0, new(optgpaxosproto.FastAcceptReply))
    mixedAcceptReplies2[2] = processMsgAndGetReply(fastAcceptChan(2), fastAccepts1[2], true, 0, new(optgpaxosproto.FastAcceptReply))

    for i := 1; i < fQ; i++{
        processMsg(fastAcceptReplyChan(0), mixedAcceptReplies2[i], true)
    }

    accepts2 := make([]fastrpc.Serializable, q)
    for i := 1; i < q; i++{
        accepts2[i] = readMessage(int32(i), new(optgpaxosproto.Accept))
    }

    acceptReplies2 := make([]fastrpc.Serializable, q)
    for i := 1; i < q; i++{
        acceptReplies2[i] = processMsgAndGetReply(acceptChan(int32(i)), accepts2[i], true, 0, new(optgpaxosproto.AcceptReply))
    }


    fmt.Printf("Pending commands before accepting command with id 1\n")
    reps[0].R.dumpDeliveryState()

    for i := 1; i < q; i++{
        processMsg(acceptReplyChan(0), acceptReplies2[i], true)
    }

    //Attention: Master is configured to deliver commands on accept. Not sure this is the best behaviour.
    fmt.Printf("Pending commands after accepting command with id 1\n")
    reps[0].R.dumpDeliveryState()

    commits2 := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++{
        commits2[i] = readMessage(int32(i), new(optgpaxosproto.Commit))
    }



    for i := 1; i < len(reps); i++{
        processMsg(commitChan(int32(i)), commits2[i], true)
    }

    reply = new(genericsmrproto.ProposeReplyTS)
    reply.Unmarshal(leaderIOChan.Reader)
    log.Printf("Reply: %v", reply)
    if reply.OK != 1 && reply.CommandId != 1{
        t.Errorf("Failed Slow Accept.")
    }

    reply = new(genericsmrproto.ProposeReplyTS)
    reply.Unmarshal(leaderIOChan.Reader)
    log.Printf("Reply: %v", reply)
    if reply.OK != 1 && reply.CommandId != 2 {
        t.Errorf("Failed Slow Accept.")
    }
}

func TestFastRoundMsgNoConflict5Replicas(t *testing.T) {
    //In this test, the fast accept message must be sent to all replicas.
    // With thirfty=true, if at least one message is reordered, then it is no longer possible to get fQ equal responses.
    setup(5, false)

    reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(0, state.PUT, 0, 0), leaderIOChan.Writer, mutex}
    <-control
    executeFastPath(false)

    reply := new(genericsmrproto.ProposeReplyTS)
    reply.Unmarshal(leaderIOChan.Reader)
    log.Printf("Reply: %v", reply)
    if reply.OK != 1 {
        t.Errorf("Failed Classic Round.")
    }

    reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(1, state.PUT, 1, 1), leaderIOChan.Writer, mutex}
    <-control

    fastAccepts1 := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++ {
        fastAccepts1[i] = readMessage(int32(i), new(optgpaxosproto.FastAccept))
    }

    reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(2, state.PUT, 1, 2), leaderIOChan.Writer, mutex}
    <-control

    fastAccepts2 := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++ {
        fastAccepts2[i] = readMessage(int32(i), new(optgpaxosproto.FastAccept))
    }

    //Mix delivery order:
    mixedAcceptReplies1 := make([]fastrpc.Serializable, len(reps))
    mixedAcceptReplies1[1] = processMsgAndGetReply(fastAcceptChan(1), fastAccepts1[1], true, 0, new(optgpaxosproto.FastAcceptReply))
    mixedAcceptReplies1[2] = processMsgAndGetReply(fastAcceptChan(2), fastAccepts2[2], true, 0, new(optgpaxosproto.FastAcceptReply))
    mixedAcceptReplies1[3] = processMsgAndGetReply(fastAcceptChan(3), fastAccepts1[3], true, 0, new(optgpaxosproto.FastAcceptReply))
    mixedAcceptReplies1[4] = processMsgAndGetReply(fastAcceptChan(4), fastAccepts1[4], true, 0, new(optgpaxosproto.FastAcceptReply))

    for i := 1; i < len(reps); i++ {
        processMsg(fastAcceptReplyChan(0), mixedAcceptReplies1[i], true)
    }

    commits1 := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++ {
        commits1[i] = readMessage(int32(i), new(optgpaxosproto.Commit))
    }

    for i := 1; i < len(reps); i++ {
        processMsg(commitChan(int32(i)), commits1[i], true)
    }

    // Deliver second pair of reordered messages
    mixedAcceptReplies2 := make([]fastrpc.Serializable, len(reps))
    mixedAcceptReplies2[1] = processMsgAndGetReply(fastAcceptChan(1), fastAccepts2[1], true, 0, new(optgpaxosproto.FastAcceptReply))
    mixedAcceptReplies2[2] = processMsgAndGetReply(fastAcceptChan(2), fastAccepts1[2], true, 0, new(optgpaxosproto.FastAcceptReply))
    mixedAcceptReplies2[3] = processMsgAndGetReply(fastAcceptChan(3), fastAccepts2[3], true, 0, new(optgpaxosproto.FastAcceptReply))
    mixedAcceptReplies2[4] = processMsgAndGetReply(fastAcceptChan(4), fastAccepts2[4], true, 0, new(optgpaxosproto.FastAcceptReply))

    for i := 1; i < len(reps); i++ {
        processMsg(fastAcceptReplyChan(0), mixedAcceptReplies1[i], true)
    }

    commits2 := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++ {
        commits2[i] = readMessage(int32(i), new(optgpaxosproto.Commit))
    }

    for i := 1; i < len(reps); i++ {
        processMsg(commitChan(int32(i)), commits2[i], true)
    }


    reply = new(genericsmrproto.ProposeReplyTS)
    reply.Unmarshal(leaderIOChan.Reader)
    log.Printf("Reply: %v", reply)
    if reply.OK != 1 && reply.CommandId != 1{
        t.Errorf("Failed Slow Accept.")
    }

    reply = new(genericsmrproto.ProposeReplyTS)
    reply.Unmarshal(leaderIOChan.Reader)
    log.Printf("Reply: %v", reply)
    if reply.OK != 1 && reply.CommandId != 2 {
        t.Errorf("Failed Slow Accept.")
    }
}

func TestNewLeadership(t *testing.T) {
    setup(5, false)

    reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(0, state.PUT, 0, 0), leaderIOChan.Writer, mutex}
    <-control
    executeFastPath(false)

    reply := new(genericsmrproto.ProposeReplyTS)
    reply.Unmarshal(leaderIOChan.Reader)
    log.Printf("Reply: %v", reply)
    if reply.OK != 1 {
        t.Errorf("Failed Classic Round.")
    }

    reps[1].R.BeTheLeader(nil, nil)

    //change order in vector, to make cycles easy
    repsTmp := reps[0]
    reps[0] = reps[1]
    reps[1] = repsTmp

    connectionsTmp := connections[0]
    connections[0] = connections[1]
    connections[1] = connectionsTmp

    newLeader := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++{
        newLeader[i] = readMessage(int32(i), new(optgpaxosproto.NewLeader))
    }

    newLeaderReplies := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++{
        newLeaderReplies[i] = processMsgAndGetReply(newLeaderChan(int32(i)), newLeader[i], true, 0, new(optgpaxosproto.NewLeaderReply))
    }

    for i := 1; i < len(reps); i++{
        processMsg(newLeaderReplyChan(0), newLeaderReplies[i], true)
    }

    syncMsg := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++{
        syncMsg[i] = readMessage(int32(i), new(optgpaxosproto.Sync))
    }

    syncReplies := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++{
        syncReplies[i] = processMsgAndGetReply(syncChan(int32(i)), syncMsg[i], true, 0, new(optgpaxosproto.SyncReply))
    }

    for i := 1; i < len(reps); i++{
        processMsg(syncReplyChan(0), syncReplies[i], true)
    }

}

//TODO: Test new leader with pending commands

//TODO: Test collision recovery with 5 replicas


func executeFastPath(thrifty bool){
    fastAccepts := make([]fastrpc.Serializable, len(reps))

    fQ := int(math.Ceil(3*float64(len(reps))/4))

    for i := 1; i < fQ; i++{
        fastAccepts[i] = readMessage(int32(i), new(optgpaxosproto.FastAccept))
    }

    fastAcceptReplies := make([]fastrpc.Serializable, fQ)
    for i := 1; i < fQ; i++{
        fastAcceptReplies[i] = processMsgAndGetReply(fastAcceptChan(int32(i)), fastAccepts[i], true, 0, new(optgpaxosproto.FastAcceptReply))
    }

    for i := 1; i < fQ; i++{
        processMsg(fastAcceptReplyChan(0), fastAcceptReplies[i], true)
    }

    commits := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++{
       commits[i] = readMessage(int32(i), new(optgpaxosproto.Commit))
    }

    for i := 1; i < len(reps); i++{
       processMsg(commitChan(int32(i)), commits[i], true)
    }

}

func processMsg(channel chan fastrpc.Serializable, msg fastrpc.Serializable, wait bool){
    channel <- msg
    if wait {
        <- control
    }

}

func processMsgAndGetReply(channel chan fastrpc.Serializable, msg fastrpc.Serializable, wait bool, rrId int32, reply fastrpc.Serializable) fastrpc.Serializable{
    processMsg(channel, msg, wait)
    readMessage(rrId, reply)
    return reply
}

func readMessage(rId int32, msg fastrpc.Serializable ) fastrpc.Serializable {
    code := make([]byte,1)
    connections[rId].is.Read(code)
    msg.Unmarshal(connections[rId].is)
    return msg
}

func createProposal(id int32, op state.Operation, k state.Key, ts int64) *genericsmrproto.Propose {
    value := make([]byte, 4)
    rand.Read(value)
    return &genericsmrproto.Propose{CommandId: id, Command: state.Command{Op: op, K: k, V: value}, Timestamp: ts}
}

func acceptChan(rId int32) chan fastrpc.Serializable {
    return reps[rId].C.acceptChan
}

func acceptReplyChan(rId int32) chan fastrpc.Serializable {
    return reps[rId].C.acceptReplyChan
}

func fastAcceptChan(rId int32) chan fastrpc.Serializable {
    return reps[rId].C.fastAcceptChan
}

func fastAcceptReplyChan(rId int32) chan fastrpc.Serializable {
    return reps[rId].C.fastAcceptReplyChan
}

func commitChan(rId int32) chan fastrpc.Serializable {
    return reps[rId].C.commitChan
}

func newLeaderChan(rId int32) chan fastrpc.Serializable {
    return reps[rId].C.newLeaderChan
}

func newLeaderReplyChan(rId int32) chan fastrpc.Serializable {
    return reps[rId].C.newLeaderReplyChan
}

func syncChan(rId int32) chan fastrpc.Serializable {
    return reps[rId].C.syncChan
}

func syncReplyChan(rId int32) chan fastrpc.Serializable {
    return reps[rId].C.syncReplyChan
}