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
)


type RepChan struct{
    C *channels
    R *Replica
}

//Assumes buffer is large enought to get messages.
const BufferSize = 4096
var connections map[int32]*connection
var reps []*RepChan
var leaderIOChan *bufio.ReadWriter
var mutex *sync.Mutex
var control = make(chan bool)

func initReplicas(count int) []*RepChan{
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
        }

        ret[i] = &RepChan{
            repiChan,
            NewReplicaStub(i, make([]string, count), isLeader, true, false, false, false, false, repiChan, connections, control),
        }

        isLeader = false
    }
    return ret
}

func setup(replicaCount int){
    reps = initReplicas(replicaCount)
    leaderChan := make(chan byte, BufferSize)
    proposeChanR := bufio.NewReader(chansmr.NewChanReader(leaderChan))
    proposeChanW := bufio.NewWriter(chansmr.NewChanWriter(leaderChan))
    leaderIOChan = bufio.NewReadWriter(proposeChanR, proposeChanW)
    mutex = &sync.Mutex{}
}

//func TestClassicRound(t *testing.T) {
//    setup(3)
//    reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(state.PUT, 0), leaderIOChan.Writer, mutex}
//    <- control
//   executeSlowPath()
//
//    reply := new(genericsmrproto.ProposeReply)
//    reply.Unmarshal(leaderIOChan.Reader)
//    log.Printf("Reply: %v", reply)
//    if reply.OK != 1 {
//        t.Errorf("Failed Classic Round.")
//    }
//}

func TestFastRound(t *testing.T) {
    setup(3)

    reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(0, state.PUT, 0, 0), leaderIOChan.Writer, mutex}
    <- control

    executeSlowPath()

    reply := new(genericsmrproto.ProposeReplyTS)
    reply.Unmarshal(leaderIOChan.Reader)
    log.Printf("Reply: %v", reply)
    if reply.OK != 1 {
        t.Errorf("Failed Classic Round.")
    }

    reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(1, state.PUT, 1, 1), leaderIOChan.Writer, mutex}
    <- control

    executeFastPath()

    reply = new(genericsmrproto.ProposeReplyTS)
    reply.Unmarshal(leaderIOChan.Reader)
    log.Printf("Reply: %v", reply)
    if reply.OK != 1 {
        t.Errorf("Failed FAST Round.")
    }
}

func executeFastPath(){
    fastAccepts := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++{
        fastAccepts[i] = readMessage(int32(i), new(optgpaxosproto.FastAccept))
    }

    fastAacceptReplies := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++{
        fastAacceptReplies[i] = processMsgAndGetReply(fastAcceptChan(1), fastAccepts[i], true, 0, new(optgpaxosproto.FastAcceptReply))
    }

    for i := 1; i < len(reps); i++{
        processMsg(fastAcceptReplyChan(0), fastAacceptReplies[i], true)
    }

    commits := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++{
       commits[i] = readMessage(int32(i), new(optgpaxosproto.Commit))
    }

    for i := 1; i < len(reps); i++{
       processMsg(commitChan(0), commits[i], true)
    }

}

func executeSlowPath(){

    prepares := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++{
        prepares[i] = readMessage(int32(i), new(optgpaxosproto.Prepare))
    }

    prepareReplies := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++{
        prepareReplies[i] = processMsgAndGetReply(prepareChan(int32(i)), prepares[i], true, 0, new(optgpaxosproto.PrepareReply))
    }

    for i := 1; i < len(reps); i++{
        processMsg(prepareReplyChan(0), prepareReplies[i], true)
    }

    accepts := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++{
        accepts[i] = readMessage(int32(i), new(optgpaxosproto.Accept))
    }

    acceptReplies := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++{
        acceptReplies[i] = processMsgAndGetReply(acceptChan(1), accepts[i], true, 0, new(optgpaxosproto.AcceptReply))
    }

    for i := 1; i < len(reps); i++{
        processMsg(acceptReplyChan(0), acceptReplies[i], true)
    }

    fullCommits := make([]fastrpc.Serializable, len(reps))
    for i := 1; i < len(reps); i++{
        fullCommits[i] = readMessage(int32(i), new(optgpaxosproto.FullCommit))
    }

    for i := 1; i < len(reps); i++{
        processMsg(fullCommitChan(int32(i)), fullCommits[i], true)
    }

}

func processMsg(channel chan fastrpc.Serializable, msg fastrpc.Serializable, wait bool){
    channel <- msg
    if wait {
        <- control
    }

}

func processMsgAndGetReply(channel chan fastrpc.Serializable, msg fastrpc.Serializable, wait bool, replyId int32, reply fastrpc.Serializable) fastrpc.Serializable{
    processMsg(channel, msg, wait)
    readMessage(replyId, reply)
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
    return &genericsmrproto.Propose{id,state.Command{op, k, value},ts}
}

func prepareChan(rId int32) chan fastrpc.Serializable {
    return reps[rId].C.prepareChan
}

func prepareReplyChan(rId int32) chan fastrpc.Serializable {
    return reps[rId].C.prepareReplyChan
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

func fullCommitChan(rId int32) chan fastrpc.Serializable {
    return reps[rId].C.fullCommitChan
}


