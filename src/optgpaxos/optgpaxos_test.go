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
const BUFFER_SIZE = 4096
var connections map[int32]*connection
var codeBuf = make([]byte, 1)
var payloadBuf = make([]byte, BUFFER_SIZE)
var reps []*RepChan
var leaderIOChan *bufio.ReadWriter
var mutex *sync.Mutex
var control = make(chan bool)

func initReplicas(count int) []*RepChan{
    ret := make([]*RepChan, count)
    isLeader := true

    connections = make(map[int32]*connection)

    for i := 0 ; i < count; i++ {
        ic := make(chan byte, BUFFER_SIZE)
        oc := ic
        connections[int32(i)] = &connection{chansmr.NewChanReader(ic), chansmr.NewChanWriter(oc)}
    }

    for i := 0 ; i < count; i++ {
        repiChan := &channels{
            make(chan *genericsmr.Propose, BUFFER_SIZE),
            make(chan fastrpc.Serializable, BUFFER_SIZE),
            make(chan fastrpc.Serializable, BUFFER_SIZE),
            make(chan fastrpc.Serializable, BUFFER_SIZE),
            make(chan fastrpc.Serializable, BUFFER_SIZE),
            make(chan fastrpc.Serializable, BUFFER_SIZE),
            make(chan fastrpc.Serializable, BUFFER_SIZE),
            make(chan fastrpc.Serializable, BUFFER_SIZE),
            make(chan fastrpc.Serializable, BUFFER_SIZE),
        }

        ret[i] = &RepChan{
            repiChan,
            NewReplicaStub(i, make([]string, count), isLeader, true, false, false, false, false, repiChan, connections, control),
        }

        isLeader = false
    }
    return ret
}

func setup(){
    reps = initReplicas(3)
    proposeChanR := bufio.NewReader(chansmr.NewChanReader(make(chan byte, BUFFER_SIZE)))
    proposeChanW := bufio.NewWriter(chansmr.NewChanWriter(make(chan byte, BUFFER_SIZE)))
    leaderIOChan = bufio.NewReadWriter(proposeChanR, proposeChanW)
    mutex = &sync.Mutex{}
}

func TestClassicRound(t *testing.T) {
    setup()
    leaderChan := make(chan byte, BUFFER_SIZE)
    proposeChanR := bufio.NewReader(chansmr.NewChanReader(leaderChan))
    proposeChanW := bufio.NewWriter(chansmr.NewChanWriter(leaderChan))
    leaderIO := bufio.NewReadWriter(proposeChanR, proposeChanW)
    mutex := &sync.Mutex{}


    reps[0].C.proposeChan <- &genericsmr.Propose{createProposal(state.PUT, 0), leaderIO.Writer, mutex}
    <- control

    prepare1 := readMessage(1, new(optgpaxosproto.Prepare))
    prepare2 := readMessage(2, new(optgpaxosproto.Prepare))

    prepareReply1 := processMsgAndGetReply(prepareChan(1), prepare1, true, 0, new(optgpaxosproto.PrepareReply))
    prepareReply2 := processMsgAndGetReply(prepareChan(2), prepare2, true, 0, new(optgpaxosproto.PrepareReply))

    processMsg(prepareReplyChan(0), prepareReply1, true)
    processMsg(prepareReplyChan(0), prepareReply2, true)

    accept1 := readMessage(1, new(optgpaxosproto.Accept))
    accept2 := readMessage(2, new(optgpaxosproto.Accept))

    acceptReply1 := processMsgAndGetReply(acceptChan(1), accept1, true, 0, new(optgpaxosproto.AcceptReply))
    acceptReply2 := processMsgAndGetReply(acceptChan(2), accept2, true, 0, new(optgpaxosproto.AcceptReply))

    processMsg(acceptReplyChan(0), acceptReply1, true)
    processMsg(acceptReplyChan(0), acceptReply2, true)

    commit1 := readMessage(1, new(optgpaxosproto.FullCommit))
    commit2 := readMessage(2, new(optgpaxosproto.FullCommit))

    processMsg(reps[1].C.fullCommitChan, commit1, true)
    processMsg(reps[2].C.fullCommitChan, commit2, true)

    reply := new(genericsmrproto.ProposeReply)
    reply.Unmarshal(leaderIO.Reader)
    log.Printf("Reply: %v", reply)
    if reply.OK != 1 {
        t.Errorf("Failed Classic Round.")
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

func createProposal(op state.Operation, k state.Key) *genericsmrproto.Propose {
    value := make([]byte, 4)
    rand.Read(value)
    return &genericsmrproto.Propose{0,state.Command{op, k, value},0}
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

func commmitChan(rId int32) chan fastrpc.Serializable {
    return reps[rId].C.commitChan
}

func fullCommitChan(rId int32) chan fastrpc.Serializable {
    return reps[rId].C.fullCommitChan
}


