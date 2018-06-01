package optgpaxos

import (
    "dlog"
    "encoding/binary"
    "fastrpc"
    "genericsmr"
    "genericsmrproto"
    "io"
    "log"
    "optgpaxosproto"
    "state"
    "time"
    "math"
    "fmt"
    "bufio"
    "chansmr"
)

//TODO: Store a sequence of commands per process, instead of a partial order?
//TODO: Add short commit again
//TODO: I disabled thrifty optimization and send messages to the quorum size.
// See broadCast operations and compare with original paxos implementation.

//const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)
const MAX_BATCH = 1

type Replica struct {
    *genericsmr.Replica // extends a generic Paxos replica
    prepareChan         chan fastrpc.Serializable
    acceptChan          chan fastrpc.Serializable
    fastAcceptChan      chan fastrpc.Serializable
    commitChan          chan fastrpc.Serializable
    syncChan          chan fastrpc.Serializable
    prepareReplyChan    chan fastrpc.Serializable
    acceptReplyChan     chan fastrpc.Serializable
    fastAcceptReplyChan chan fastrpc.Serializable
    syncReplyChan     chan fastrpc.Serializable
    prepareRPC          uint8
    acceptRPC           uint8
    fastAcceptRPC       uint8
    commitRPC           uint8
    syncRPC             uint8
    fullCommitRPC       uint8
    commitShortRPC      uint8
    prepareReplyRPC     uint8
    acceptReplyRPC      uint8
    fastAcceptReplyRPC  uint8
    syncReplyRPC        uint8
    status              Status
    /*IsLeader            bool*/        // does this replica think it is the leader
    /*instanceSpace       map[int32]*Instance*/ // the space of all instances (used and not yet used)
    /*crtInstance         int32*/       // highest active instance number that this replica knows about
    defaultBallot       int32       // default ballot for new instances (0 until a Prepare(ballot, instance->infinity) from a leader)
    maxRecvBallot       int32
    currBallot          int32
    Shutdown            bool
    /*counter             int*/
    flush               bool
    /*committedUpTo       int32*/
    forceNewBallot      bool
    cmds     map[state.Id][]state.Command
    deps     map[state.Id][]state.Id
    phase    map[state.Id]Phase
    lb       *LeaderBookkeeping
}

type Status int
type Phase int

const (
    start Phase = iota
    fastAcceptMode
    slowAcceptMode
    committed
    forDelivery
    delivered
)

const (
    preparing Status = iota
    follower
    leader
)

//TODO: Check which are not necessary
type LeaderBookkeeping struct {
    proposalsById       map[state.Id][]*genericsmr.Propose
    prepareOKs          map[state.Id]int
    fastAcceptOKs       map[state.Id]int
    fastAcceptNOKs      map[state.Id]int
    acceptOKs           map[state.Id]int
    acceptNOKs          map[state.Id]int
}


type channels struct {
    proposeChan         chan *genericsmr.Propose
    prepareChan         chan fastrpc.Serializable
    acceptChan          chan fastrpc.Serializable
    fastAcceptChan      chan fastrpc.Serializable
    commitChan          chan fastrpc.Serializable
    syncChan      chan fastrpc.Serializable
    prepareReplyChan    chan fastrpc.Serializable
    acceptReplyChan     chan fastrpc.Serializable
    fastAcceptReplyChan chan fastrpc.Serializable
    syncReplyChan      chan fastrpc.Serializable
}

type connection struct{
    is	*chansmr.ChanReader
    os  *chansmr.ChanWriter
}

func NewReplicaStub(id int, peerAddrList []string, IsLeader bool, thrifty bool, exec bool, lread bool, dreply bool, durable bool, chans *channels, connections map[int32]*connection, control chan bool) *Replica {
    r := &Replica{genericsmr.NewReplica(id, peerAddrList, thrifty, exec, lread, dreply),
        chans.prepareChan,
        chans.acceptChan,
        chans.fastAcceptChan,
        chans.commitChan,
        chans.syncChan,
        chans.prepareReplyChan,
        chans.acceptReplyChan,
        chans.fastAcceptReplyChan,
        chans.syncReplyChan,
        0, 0, 0,0,0, 0, 0, 0, 0, 0, 0,
        follower,
        -1,
        -1,
        -1,
        false,
        false,
        true,
        make(map[state.Id][]state.Command),
        make(map[state.Id][]state.Id),
        make(map[state.Id]Phase),
        nil,
    }


    r.Durable = durable
    r.ProposeChan = chans.proposeChan

    if IsLeader {
        r.lb = &LeaderBookkeeping{
            make(map[state.Id][]*genericsmr.Propose),
            make(map[state.Id]int),
            make(map[state.Id]int),
            make(map[state.Id]int),
            make(map[state.Id]int),
            make(map[state.Id]int),
        }
        r.status = leader
    }

    r.prepareRPC = r.RegisterRPC(new(optgpaxosproto.Prepare), r.prepareChan)
    r.acceptRPC = r.RegisterRPC(new(optgpaxosproto.Accept), r.acceptChan)
    r.fastAcceptRPC = r.RegisterRPC(new(optgpaxosproto.FastAccept), r.fastAcceptChan)
    r.commitRPC = r.RegisterRPC(new(optgpaxosproto.Commit), r.commitChan)
    r.syncRPC = r.RegisterRPC(new(optgpaxosproto.Sync), r.syncChan)
    r.prepareReplyRPC = r.RegisterRPC(new(optgpaxosproto.PrepareReply), r.prepareReplyChan)
    r.acceptReplyRPC = r.RegisterRPC(new(optgpaxosproto.AcceptReply), r.acceptReplyChan)
    r.fastAcceptReplyRPC = r.RegisterRPC(new(optgpaxosproto.FastAcceptReply), r.fastAcceptReplyChan)
    r.syncReplyRPC = r.RegisterRPC(new(optgpaxosproto.SyncReply), r.syncReplyChan)

    go r.runFake(connections, control)

    return r
}

//func NewReplica(id int, peerAddrList []string, Isleader bool, thrifty bool, exec bool, lread bool, dreply bool, durable bool) *Replica {
//    r := &Replica{genericsmr.NewReplica(id, peerAddrList, thrifty, exec, lread, dreply),
//        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
//        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
//        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
//        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
//        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
//        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
//        make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
//        make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
//        0, 0, 0, 0, 0, 0, 0, 0, 0,
//        false,
//        make([]*Instance, 15*1024*1024),
//        0,
//        -1,
//        -1,
//        false,
//        0,
//        true,
//        -1,
//        false,
//        -1,
//    }
//
//    r.Durable = durable
//    r.IsLeader = Isleader
//
//    r.prepareRPC = r.RegisterRPC(new(optgpaxosproto.Prepare), r.prepareChan)
//    r.acceptRPC = r.RegisterRPC(new(optgpaxosproto.Accept), r.acceptChan)
//    r.fastAcceptRPC = r.RegisterRPC(new(optgpaxosproto.FastAccept), r.fastAcceptChan)
//    r.commitRPC = r.RegisterRPC(new(optgpaxosproto.Commit), r.commitChan)
//    r.fullCommitRPC = r.RegisterRPC(new(optgpaxosproto.FullCommit), r.fullCommitChan)
//    r.prepareReplyRPC = r.RegisterRPC(new(optgpaxosproto.PrepareReply), r.prepareReplyChan)
//    r.acceptReplyRPC = r.RegisterRPC(new(optgpaxosproto.AcceptReply), r.acceptReplyChan)
//    r.fastAcceptReplyRPC = r.RegisterRPC(new(optgpaxosproto.FastAcceptReply), r.fastAcceptReplyChan)
//
//    go r.run()
//
//    return r
//}

//append a log entry to stable storage
//TODO: must understand the semantics of the log. Should store deps?
func (r *Replica) recordInstanceMetadata(id state.Id) {
    if !r.Durable {
        return
    }
    var b [13]byte
    binary.LittleEndian.PutUint32(b[0:4], uint32(r.currBallot))
    binary.LittleEndian.PutUint64(b[4:12], uint64(id))
    b[12] = byte(r.phase[id])
    r.StableStore.Write(b[:])
}

//write a sequence of commands to stable storage
func (r *Replica) recordCommands(cmds []state.Command) {
    if !r.Durable {
        return
    }

    if cmds == nil {
        return
    }
    for i := 0; i < len(cmds); i++ {
        cmds[i].Marshal(io.Writer(r.StableStore))
    }
}


//sync with the stable store
func (r *Replica) sync() {
    if !r.Durable {
        return
    }

    r.StableStore.Sync()
}

/* RPC to be called by master */

func (r *Replica) BeTheLeader(args *genericsmrproto.BeTheLeaderArgs, reply *genericsmrproto.BeTheLeaderReply) error {
    r.Mutex.Lock()
    r.status = leader
    r.lb = &LeaderBookkeeping{
        make(map[state.Id][]*genericsmr.Propose),
        make(map[state.Id]int),
        make(map[state.Id]int),
        make(map[state.Id]int),
        make(map[state.Id]int),
        make(map[state.Id]int),
    }
    r.forceNewBallot = true

    log.Println("I am the leader")
    time.Sleep(5 * time.Second) // wait that the connection is actually lost
    r.Mutex.Unlock()
    return nil
}

func (r *Replica) revokeLeader(){
    log.Println("No longer leader")
    r.status = follower
    r.lb = nil
}

func (r *Replica) replyPrepare(replicaId int32, reply *optgpaxosproto.PrepareReply) {
    r.SendMsg(replicaId, r.prepareReplyRPC, reply)
}

func (r *Replica) replyAccept(replicaId int32, reply *optgpaxosproto.AcceptReply) {
    r.SendMsg(replicaId, r.acceptReplyRPC, reply)
}

func (r *Replica) replyFastAccept(replicaId int32, reply *optgpaxosproto.FastAcceptReply) {
    r.SendMsg(replicaId, r.fastAcceptReplyRPC, reply)
}

func (r *Replica) replySync(replicaId int32, reply *optgpaxosproto.SyncReply) {
    r.SendMsg(replicaId, r.syncReplyRPC, reply)
}


/* ============= */

var clockChan chan bool

func (r *Replica) clock() {
    for !r.Shutdown {
        time.Sleep(1000 * 1000 * 5)
        clockChan <- true
    }
}

/* Main event processing loop */

func (r *Replica) run() {

    r.ConnectToPeers()

    r.ComputeClosestPeers()

    if r.status == leader {
        log.Println("I am the leader")
    }

    if r.Exec {
        go r.executeCommands()
    }

    clockChan = make(chan bool, 1)
    go r.clock()

    go r.WaitForClientConnections()

    r.messageLoop(nil)
}

func (r *Replica) runFake(connections map[int32]*connection, control chan bool) {

    for i := 0; i < len(connections); i++ {
        r.Alive[i] = true
        r.PeerReaders[i] = bufio.NewReader(connections[int32(i)].is)
        r.PeerWriters[i] = bufio.NewWriter(connections[int32(i)].os)
        log.Printf("OUT Connected to %d", i)
    }
    log.Printf("Replica id: %d. Done connecting to peers\n", r.Id)

    //if r.Exec {
    //    go r.executeCommands()
    //}

    clockChan = make(chan bool, 1)
    go r.clock()
    r.messageLoop(control)
}

func (r *Replica) messageLoop(control chan bool) {
    onOffProposeChan := r.ProposeChan
    for !r.Shutdown {
        select {

        case <-clockChan:
            //activate the new proposals channel
            onOffProposeChan = r.ProposeChan
            break

        case propose := <-onOffProposeChan:
            //got a Propose from a client
            dlog.Printf("%d: Received proposal with type=%d\n", r.Id, propose.Command.Op)
            r.handlePropose(propose)
            //deactivate the new proposals channel to prioritize the handling of protocol messages
            sendControl(control)
            onOffProposeChan = nil
            break

        case prepareS := <-r.prepareChan:
            prepare := prepareS.(*optgpaxosproto.Prepare)
            //got a Prepare message
            dlog.Printf("%d: Received Prepare from replica %d, with ballot %d\n", r.Id, prepare.Ballot)
            r.handlePrepare(prepare)
            sendControl(control)
            break

        case acceptS := <-r.acceptChan:
            accept := acceptS.(*optgpaxosproto.Accept)
            //got an Accept message
            dlog.Printf("%d: Received Accept from replica %d, for ballot %d with id %d\n", r.Id, accept.LeaderId, accept.Ballot, accept.Id)
            r.handleAccept(accept)
            sendControl(control)
            break

        case fastAcceptS := <-r.fastAcceptChan:
            fastAccept := fastAcceptS.(*optgpaxosproto.FastAccept)
            //got an Accept message
            dlog.Printf("%d: Received Fast Accept from replica %d, for ballot %d with id %d\n", r.Id, fastAccept.LeaderId, fastAccept.Ballot, fastAccept.Id)
            r.handleFastAccept(fastAccept)
            sendControl(control)
            break

        case commitS := <-r.commitChan:
            commit := commitS.(*optgpaxosproto.Commit)
            //got a Commit message
            dlog.Printf("%d: Received Commit from replica %d, with id %d\n", r.Id, commit.LeaderId, commit.Id)
            r.handleCommit(commit)
            sendControl(control)
            break

        case syncS := <-r.syncChan:
            sync := syncS.(*optgpaxosproto.Sync)
            dlog.Printf("%d: Received Full Commit from replica %d, for ballot %d\n", r.Id, sync.LeaderId, sync.Ballot)
            r.handleSync(sync)
            sendControl(control)
            break

        case prepareReplyS := <-r.prepareReplyChan:
            prepareReply := prepareReplyS.(*optgpaxosproto.PrepareReply)
            //got a Prepare reply
            dlog.Printf("%d: Received PrepareReply for ballot %d\n", r.Id, prepareReply.Ballot)
            r.handlePrepareReply(prepareReply)
            sendControl(control)
            break

        case acceptReplyS := <-r.acceptReplyChan:
            acceptReply := acceptReplyS.(*optgpaxosproto.AcceptReply)
            //got an Accept reply
            dlog.Printf("%d: Received AcceptReply for ballot %d\n", r.Id, acceptReply.Ballot)
            r.handleAcceptReply(acceptReply)
            sendControl(control)
            break

        case fastAcceptReplyS := <-r.fastAcceptReplyChan:
            fastAcceptReply := fastAcceptReplyS.(*optgpaxosproto.FastAcceptReply)
            //got an Accept reply
            dlog.Printf("%d: Received FastAcceptReply for ballot %d\n", r.Id, fastAcceptReply.Ballot)
            r.handleFastAcceptReply(fastAcceptReply)
            sendControl(control)
            break
        }

    }
}

func sendControl(control chan bool){
    //log.Printf("Sent Control")
    if control !=  nil{
        control <- true
    }
}

func (r *Replica) makeBallot(instance int32) int32 {
    ret := r.Id

    if r.defaultBallot > ret {
        ret = r.defaultBallot + 1
    }

    if r.maxRecvBallot > ret {
        ret = r.maxRecvBallot + 1
    }

    return ret
}

func (r *Replica) makeBallot2(forceNew bool) int32 {
    if r.currBallot == -1 {
        r.currBallot = r.Id
    }

    if r.maxRecvBallot > r.currBallot {
        r.currBallot = r.maxRecvBallot + 1
    } else if forceNew{
        r.currBallot++
    }

    return r.currBallot
}

//func (r *Replica) updateCommittedUpTo() {
//   for r.instanceSpace[r.committedUpTo+1] != nil{
//       if r.instanceSpace[r.committedUpTo+1].status == FINISHED || r.instanceSpace[r.committedUpTo+1].status == FAST_ACCEPT {
//           r.committedUpTo++
//       }
//   }
//   dlog.Printf("%d Committed up to %d", r.Id, r.committedUpTo)
//}



// Gets dependencies from previous instances and current instance.
//TODO: Splitting commands over multiple instances reduces the cost of this step.
func (r *Replica) getDeps(cmd state.Command) []state.Id {
    deps := make([]state.Id, 0)
    for id, other := range r.cmds {
        if isConflicting(cmd, other) && r.phase[id] != start {
            deps = append(deps, id)
        }
    }
    return deps
}

func (r *Replica) checkDependenciesDelived(id state.Id, instNo int32) bool {
    ret := true
    var failed string
    for _, deps := range r.deps {
        for _, d := range deps{
            if r.phase[d] != delivered  {
                ret = false
                failed = fmt.Sprintf("Missing %v in instance %d", d, instNo)
                break
            } else {
                ret = true
            }
        }
    }
    if !ret{
        dlog.Printf("Failed obtaining dependencies for %d: %v", id, failed)
    }
    return ret
}


func isConflicting(cmd state.Command, otherCmds []state.Command) bool {
    for _, other := range otherCmds {
        if cmd.K == other.K && (cmd.Op == state.PUT || other.Op == state.PUT) {
            return true
        }
    }
    return false
}

func (r *Replica) getFullCommands() *state.FullCmds {
    fullCmds := &state.FullCmds{
        Id: make([]state.Id, len(r.cmds)),
        C:  make([][]state.Command, len(r.cmds)),
        D:  make([][]state.Id, len(r.cmds)),
        P:  make([]state.Phase, len(r.cmds)),
    }

    i := 0
    for id, cmd := range r.cmds{
        fullCmds.Id[i] = id
        fullCmds.C[i] = cmd
        fullCmds.D[i] = r.deps[id]
        fullCmds.P[i] = state.Phase(r.phase[id])
        i++
    }

    return fullCmds
}

func (r *Replica) getSeparateCommands(fullCmds *state.FullCmds) (map[state.Id][]state.Command, map[state.Id][]state.Id, map[state.Id]Phase)  {
    C := make(map[state.Id][]state.Command)
    D := make(map[state.Id][]state.Id)
    P := make(map[state.Id]Phase)

    for i, id := range fullCmds.Id{
        C[id] = fullCmds.C[i]
        D[id] = fullCmds.D[i]
        P[id] = Phase(fullCmds.P[i])
    }

    return C, D, P
}

//func newInstance(createLb bool) *Instance{
//    var lb *LeaderBookkeeping
//
//    if(createLb){
//        lb = &LeaderBookkeeping{
//            make(map[state.Id][]*genericsmr.Propose),
//            0,
//            make(map[state.Id]int),
//            make(map[state.Id]int),
//            0,
//            0,
//        }
//    }
//
//    inst := &Instance{
//        make(map[state.Id][]state.Command),
//        make(map[state.Id][]state.Id),
//        -1,
//        make(map[state.Id]Phase),
//        -1,
//        lb,
//
//    }
//    return inst
//}

func (r *Replica) bcast(quorum int, channel uint8, msg fastrpc.Serializable) {
    defer func() {
        if err := recover(); err != nil {
            log.Println("Prepare bcast failed:", err)
        }
    }()

    sent := 0
    for q := 0; q < r.N-1; q++ {
        if !r.Alive[r.PreferredPeerOrder[q]] {
            continue
        }
        r.SendMsg(r.PreferredPeerOrder[q], channel, msg)
        dlog.Printf("%d sent message to %d", r.Id, r.PreferredPeerOrder[q])
        sent++
        if sent >= quorum {
            break
        }
    }

}

func (r *Replica) bcastPrepare(ballot int32, toInfinity bool) {
    dlog.Printf("Send 1A message")
    ti := FALSE
    if toInfinity {
        ti = TRUE
    }
    args := &optgpaxosproto.Prepare{LeaderId: r.Id, Ballot: ballot, ToInfinity: ti}

    n := r.N
    if r.Thrifty {
        n = int(math.Ceil(float64(n)/2))
    }

    r.bcast(n, r.prepareRPC, args)

}

func (r *Replica) bcastFastAccept(ballot int32, id state.Id, cmd []state.Command) {
    dlog.Printf("Send 2A Fast message")
    args := &optgpaxosproto.FastAccept{ LeaderId: r.Id, Ballot: ballot, Id: id, Cmd: cmd}

    n := r.N
    if r.Thrifty {
        n = int(math.Ceil(3*float64(n)/4))-1
    }

    r.bcast(n, r.fastAcceptRPC, args)
}


func (r *Replica) bcastAccept(ballot int32, id state.Id, cmd []state.Command, deps []state.Id) {
    dlog.Printf("Send 2A message")

    args := &optgpaxosproto.Accept{LeaderId: r.Id, Ballot: ballot, Id: id, Cmd: cmd, Dep: deps}

    n := r.N
    if r.Thrifty {
        n = int(math.Ceil(float64(n)/2))
    }
    r.bcast(n, r.acceptRPC, args)
}

func (r *Replica) bcastCommit(ballot int32, id state.Id, cmd []state.Command, deps []state.Id) {
    defer func() {
        if err := recover(); err != nil {
            log.Println("Commit bcast failed:", err)
        }
    }()

    args := &optgpaxosproto.Commit{LeaderId: r.Id, Id: id, Cmd: cmd, Deps: deps}

    r.bcast(r.N-1, r.commitRPC, args)

}

func (r *Replica) bcastSync(ballot int32) {
    defer func() {
        if err := recover(); err != nil {
            log.Println("Commit bcast failed:", err)
        }
    }()
    args := &optgpaxosproto.Sync{LeaderId: r.Id, Ballot: ballot, Cmds: *r.getFullCommands()}

    r.bcast(r.N-1, r.fullCommitRPC, args)
}

func (r *Replica) handlePropose(propose *genericsmr.Propose) {
    if r.status != leader {
        dlog.Printf("Not the leader, cannot propose %v\n", propose.CommandId)
        preply := &genericsmrproto.ProposeReplyTS{OK: FALSE, CommandId: -1, Value: state.NIL()}
        r.ReplyProposeTS(preply, propose.Reply, propose.Mutex)
        return
    }

    dlog.Printf("Handle propose")

    batchSize := len(r.ProposeChan) + MAX_BATCH

    if batchSize > MAX_BATCH {
        batchSize = MAX_BATCH
    }

    dlog.Printf("Batched %d", batchSize)

    id := state.Id(propose.CommandId)

    if(r.phase[id] != start){
        if(r.lb.proposalsById[id] != nil){
            for _, p := range r.lb.proposalsById[id] {
                r.deliverCommand(p, FALSE, state.NIL())
                r.lb.proposalsById[id] = nil // hack to ensure reply only once
            }
        }
        return
    }
    dlog.Printf("ID for command: %d", id)

    cmds := make([]state.Command, batchSize)
    proposals := make([]*genericsmr.Propose, batchSize)
    cmds[0] = propose.Command
    proposals[0] = propose

    for i := 1; i < batchSize; i++ {
        prop := <-r.ProposeChan
        cmds[i] = propose.Command
        proposals[i] = prop
    }

    currBallot := r.currBallot
    ballot := r.makeBallot2(r.forceNewBallot)
    r.forceNewBallot = false

    r.cmds[id] = cmds
    r.lb.proposalsById[id] = proposals

    //TODO: I don't think this works if multiple nodes think they are leaders.
    if r.currBallot != ballot && currBallot != -1 {
        dlog.Printf("Classic round for cmd id: %d, with ballot %d\n", id, ballot)
        r.phase[id] = slowAcceptMode
        r.bcastPrepare(ballot, true)
    } else {
        dlog.Printf("Fast round for cmd id: %d, with ballot %d\n", id, ballot)
        var deps []state.Id
        for _, cmd := range cmds {
            deps = append(deps, r.getDeps(cmd)...)
        }
        r.deps[id] = deps
        r.phase[id] = fastAcceptMode
        r.lb.fastAcceptOKs[id] = 1
        r.lb.fastAcceptNOKs[id] = 0
        r.lb.acceptOKs[id] = 0
        r.lb.acceptNOKs[id] = 0

        r.recordInstanceMetadata(id)
        r.recordCommands(r.cmds[id])
        r.sync()

        r.bcastFastAccept(ballot, id, cmds)
    }
}

func (r *Replica) handlePrepare(prepare *optgpaxosproto.Prepare) {
    var preply *optgpaxosproto.PrepareReply

    r.maxRecvBallot = max(prepare.Ballot, r.maxRecvBallot)

    if prepare.Ballot > r.currBallot {
        preply = &optgpaxosproto.PrepareReply{Ballot: prepare.Ballot, OK: TRUE, Cmds: *r.getFullCommands()}
        r.currBallot = prepare.Ballot
        r.status = preparing

    } else {
        dlog.Printf("Lower than default! %d < %d", prepare.Ballot, r.defaultBallot)
        preply = &optgpaxosproto.PrepareReply{Ballot: r.defaultBallot, OK: FALSE, Cmds: state.FullCmds{}}
    }

    //Ignore message if NOK?
    r.replyPrepare(prepare.LeaderId, preply)

    //TODO: what does ToInfinity do?
    /*if prepare.ToInfinity == TRUE && prepare.Ballot > r.defaultBallot {
        r.defaultBallot = prepare.Ballot
    }*/
}

func (r *Replica) handleFastAccept(fastAccept *optgpaxosproto.FastAccept) {
    if r.status == leader && r.cmds[fastAccept.Id] != nil {
        dlog.Printf("I am the leader for this command, I should not handle FAST Accept: %v\n", fastAccept.Id)
        return
    }
    var fareply *optgpaxosproto.FastAcceptReply
    r.maxRecvBallot = max(fastAccept.Ballot, r.maxRecvBallot)
    id := fastAccept.Id
    if fastAccept.Ballot < r.currBallot{
        fareply = &optgpaxosproto.FastAcceptReply{Ballot: r.currBallot, OK: FALSE, Id: fastAccept.Id}
    } else {
        // FAST FORWARD if fastAccept.ballot > r.currBallot. Should we handle it differently?
        if r.phase[id] == start {
            var deps []state.Id
            for _, cmd := range fastAccept.Cmd {
                deps = append(deps, r.getDeps(cmd)...)
            }
            r.cmds[id] = fastAccept.Cmd
            r.deps[id] = deps
            r.phase[id] = fastAcceptMode
            r.currBallot = fastAccept.Ballot
            fareply = &optgpaxosproto.FastAcceptReply{Ballot: r.currBallot, OK: TRUE, Id: id, Cmd: r.cmds[id], Deps: r.deps[id]}
        } else {
            dlog.Printf("Current instance has already started.")
            //if r.phase[id] == FAST_ACCEPT{
            //    dlog.Printf("Reply already sent. Replying again.")
            //    areply = &optgpaxosproto.FastAcceptReply{TRUE, r.currBallot, id, deps}
            //    //It seems that the code does not check repeated responses, so it should not.
            //}
            fareply = &optgpaxosproto.FastAcceptReply{Ballot: r.currBallot, OK: FALSE, Id: id, Deps: nil}
        }

    }

    if fareply.OK == TRUE {
        r.recordInstanceMetadata(id)
        r.recordCommands(fastAccept.Cmd)
        r.sync()
    }

    dlog.Printf("%d Response for %d :  %d, %+v", r.Id, fastAccept.Id, fareply.OK, fareply.Deps)
    r.replyFastAccept(fastAccept.LeaderId, fareply)
}

func (r *Replica) handleAccept(accept *optgpaxosproto.Accept) {
    if r.status == leader && r.cmds[accept.Id] != nil {
        dlog.Printf("I am the leader for this command, I should not handle FAST Accept: %v\n", accept.Id)
        return
    }
    var areply *optgpaxosproto.AcceptReply
    r.maxRecvBallot = max(accept.Ballot, r.maxRecvBallot)
    id := accept.Id

    if accept.Ballot < r.currBallot{
        areply = &optgpaxosproto.AcceptReply{Ballot: r.currBallot, OK: FALSE, Id: accept.Id}
    } else {
        // FAST FORWARD if fastAccept.ballot > r.currBallot. Should we handle it differently?
        var deps []state.Id
        r.cmds[id] = accept.Cmd
        r.deps[id] = deps
        r.phase[id] = slowAcceptMode
        r.currBallot = accept.Ballot
        areply = &optgpaxosproto.AcceptReply{Ballot: r.currBallot, OK: TRUE, Id: id, Cmd: r.cmds[id], Dep: r.deps[id]}


    }

    if areply.OK == TRUE {
        r.recordInstanceMetadata(id)
        r.recordCommands(accept.Cmd)
        r.sync()
    }

    //Reply even if NOK
    dlog.Printf("%d Response for %d :  %d", r.Id, accept.Id, areply.OK)
    r.replyAccept(accept.LeaderId, areply)

}

func (r *Replica) handleCommit(commit *optgpaxosproto.Commit) {
    id := commit.Id
    r.cmds[id] = commit.Cmd
    r.deps[id] = commit.Deps
    r.phase[id] = committed

    r.recordInstanceMetadata(id)
    r.recordCommands(r.cmds[id])

    dlog.Printf("Pending Commands %d", len(r.ProposeChan))

}

func (r *Replica) handleSync(sync *optgpaxosproto.Sync) {

    C,D,P := r.getSeparateCommands(&sync.Cmds)
    r.maxRecvBallot = max(sync.Ballot, r.maxRecvBallot)

    if sync.Ballot > r.currBallot{
        r.cmds = C
        r.deps = D
        r.phase = P

        for id := range C {
            r.recordInstanceMetadata(id)
            r.recordCommands(r.cmds[id])
        }

        sreply := &optgpaxosproto.SyncReply{LeaderId: sync.LeaderId, OK: FALSE, Ballot: r.currBallot}
        r.replySync(sync.LeaderId, sreply)
    } else{

    }
}

func (r *Replica) handlePrepareReply(preply *optgpaxosproto.PrepareReply) {

    r.maxRecvBallot = max(preply.Ballot, r.maxRecvBallot)

    if r.status != preparing  || preply.Ballot != r.currBallot {
        // TODO: should replies for non-current ballots be ignored?
        // we've moved on -- these are delayed replies, so just ignore
        dlog.Printf("Ignoring instances not in PREPARING stage")
        return
    }


    //TODO: Recovery not implemented.
}

func (r *Replica) handleFastAcceptReply(areply *optgpaxosproto.FastAcceptReply) {
    id := areply.Id
    r.maxRecvBallot = max(areply.Ballot, r.maxRecvBallot)

    if r.status != leader {
        dlog.Printf("Received FastAcceptReply but I am not the leader")
        return;
    }

    if r.phase[id] != fastAcceptMode {
        dlog.Printf("Message ignored. %d is not in fast accept phase", id)
        // we've move on, these are delayed replies, so just ignore
    }

    if areply.OK == TRUE && r.currBallot == areply.Ballot {
        allEqual := true
        if len(r.deps[id]) != len(areply.Deps){
            allEqual = false
        } else {
            for _, depi := range areply.Deps {
                allEqual = false;
                for _, depj := range areply.Deps {
                    if depi == depj {
                        allEqual = true
                        break;
                    }
                }
                if !allEqual{
                    dlog.Printf("Dep check NOK for id %+v: %v != %+v", id, r.deps[id], areply.Deps)
                    break
                }
            }

        }
        if allEqual {
            r.lb.fastAcceptOKs[id]++
            dlog.Printf("Min OKs > %d, got: %d", int(math.Ceil(3*float64(r.N)/4))-1, r.lb.fastAcceptOKs[id])

        } else{
            r.lb.fastAcceptNOKs[id]++
            dlog.Printf("Max NOKs : %d, got: %d", int(math.Ceil(1*float64(r.N)/4))-1, r.lb.fastAcceptNOKs[id])
        }
        if r.lb.fastAcceptOKs[id] > int(math.Ceil(3*float64(r.N)/4))-1 {
            r.phase[id] = committed
            r.bcastCommit(r.currBallot, id, r.cmds[id], r.deps[id])

            if r.lb.proposalsById[id] != nil{
                r.tryToDeliverOrExecute(!r.Dreply, false)
            }

            r.recordInstanceMetadata(id)
            r.sync() //is this necessary?
        } else if r.lb.fastAcceptNOKs[areply.Id] > int(math.Ceil(1*float64(r.N)/4)-1){
            dlog.Printf("Collision recovery")
            r.phase[id] = slowAcceptMode
            r.lb.acceptOKs[id] = 1
            r.lb.acceptNOKs[id] = 0
            r.recordInstanceMetadata(id)
            r.sync()
            r.bcastAccept(r.currBallot, id, r.cmds[id], r.deps[id])
        }
    } else {
        if(r.maxRecvBallot > r.currBallot){
            dlog.Printf("There is another active leader ( %d > %d)", r.maxRecvBallot, r.currBallot, ")")
            //TODO: Must test leader change.
            if(r.status == leader){
                r.revokeLeader()
            }
        }
    }
}

func (r *Replica) handleAcceptReply(areply *optgpaxosproto.AcceptReply) {
    id := areply.Id
    r.maxRecvBallot = max(areply.Ballot, r.maxRecvBallot)

    if r.phase[id] != slowAcceptMode {
        dlog.Printf("Message ignored. %d is not in slow accept phase", id)
        // we've move on, these are delayed replies, so just ignore
        return
    }

    if r.status != leader {
        dlog.Printf("Received AcceptReply but I am not the leader")
        return;
    }

    if areply.OK == TRUE && r.currBallot == areply.Ballot {
        r.lb.acceptOKs[id]++
        //TODO: What is the size of this quorum?
        if r.lb.acceptOKs[id] > r.N>>1 {
            r.phase[id] = committed
            r.bcastCommit(r.currBallot, id, r.cmds[id], r.deps[id])

            if r.lb.proposalsById[id] != nil{
                r.tryToDeliverOrExecute(!r.Dreply, false)
            }

            r.recordInstanceMetadata(id)
            r.sync() //is this necessary?
        }
    } else {
        dlog.Printf("There is another active leader (", areply.Ballot, " > ", r.maxRecvBallot, ")")
        r.lb.acceptNOKs[id]++
        //Do nothing
    }
}

func (r *Replica) executeCommands() {
    for !r.Shutdown {
        executed := r.tryToDeliverOrExecute(r.Dreply && r.status == leader, true)

        if !executed { // I think it can sleep everytime, becuase method delivers all possible
            time.Sleep(1* time.Second)
        }

    }

}

func (r *Replica) tryToDeliverOrExecute(deliver bool, execute bool) bool{
    //TODO: Cache undelivered for faster retries
    anyDelivered := true
    atLeastOne := false
    for anyDelivered {
        anyDelivered = false
        for id, phase := range r.phase{
            if phase == committed{
                allDepsDelivered := true
                for _, d := range r.deps[id]{
                    if r.phase[d] != delivered{
                        allDepsDelivered = false
                        break
                    }
                }
                if allDepsDelivered {
                    r.phase[id] = forDelivery
                    if deliver || execute{
                        for i, c := range r.cmds[id]{
                            val := state.NIL()
                            if execute {
                                val = c.Execute(r.State)
                            }
                            if deliver {
                                r.deliverCommand(r.lb.proposalsById[id][i], TRUE, val)
                                r.phase[id] = delivered
                            }
                        }

                    }
                    anyDelivered = true
                    atLeastOne = true
                }
            }
        }
    }
    return atLeastOne
}

func (r *Replica) deliverCommand(p *genericsmr.Propose, ok uint8, val state.Value){
        propreply := &genericsmrproto.ProposeReplyTS{
            OK:        ok,
            CommandId: p.CommandId,
            Value:     val,
            Timestamp: p.Timestamp}
        r.ReplyProposeTS(propreply, p.Reply, p.Mutex)


}

func max(x, y int32) int32 {
    if x > y {
        return x
    }
    return y
}
