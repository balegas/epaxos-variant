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
)

//TODO: Store a sequence of commands per process, instead of a partial order?
//TODO: Add short commit again
//TODO: I disabled thrifty optimization and send messages to the quorum size.
// See broadCast operations and compare with original paxos implementation.

//const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)

const MAX_BATCH = 5000

type Replica struct {
    *genericsmr.Replica // extends a generic Paxos replica
    prepareChan         chan fastrpc.Serializable
    acceptChan          chan fastrpc.Serializable
    fastAcceptChan      chan fastrpc.Serializable
    commitChan          chan fastrpc.Serializable
    //commitShortChan     chan fastrpc.Serializable
    fullCommitChan      chan fastrpc.Serializable
    prepareReplyChan    chan fastrpc.Serializable
    acceptReplyChan     chan fastrpc.Serializable
    fastAcceptReplyChan chan fastrpc.Serializable
    prepareRPC          uint8
    acceptRPC           uint8
    fastAcceptRPC       uint8
    commitRPC           uint8
    fullCommitRPC       uint8
    commitShortRPC      uint8
    prepareReplyRPC     uint8
    acceptReplyRPC      uint8
    fastAcceptReplyRPC  uint8
    IsLeader            bool        // does this replica think it is the leader
    instanceSpace       []*Instance // the space of all instances (used and not yet used)
    crtInstance         int32       // highest active instance number that this replica knows about
    defaultBallot       int32       // default ballot for new instances (0 until a Prepare(ballot, instance->infinity) from a leader)
    maxRecvBallot       int32
    Shutdown            bool
    counter             int
    flush               bool
    committedUpTo       int32
    forceNewBallot      bool
    lastId              state.Id
}

type InstanceStatus int
type Phase int

const (
    START Phase = iota
    ACCEPTED
    COMMITTED
    DELIVERED
)

const (
    PREPARING InstanceStatus = iota
    PREPARED
    FAST_ACCEPT
    SLOW_ACCEPT
    FINISHED
)

type Instance struct {
    cmds     map[state.Id][]state.Command
    deps     map[state.Id][]state.Id
    ballot   int32
    phase    map[state.Id]Phase
    status   InstanceStatus
    lb       *LeaderBookkeeping
    //orderedIds []state.Id
}

type LeaderBookkeeping struct {
    proposalsById       map[state.Id][]*genericsmr.Propose
    prepareOKs      int
    fastAcceptOKs       map[state.Id]int
    fastAcceptNOKs       map[state.Id]int
    acceptOKs       int
    nacks           int
}

func NewReplica(id int, peerAddrList []string, Isleader bool, thrifty bool, exec bool, lread bool, dreply bool, durable bool) *Replica {
    r := &Replica{genericsmr.NewReplica(id, peerAddrList, thrifty, exec, lread, dreply),
        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
        make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
        0, 0, 0, 0, 0, 0, 0, 0, 0,
        false,
        make([]*Instance, 15*1024*1024),
        0,
        -1,
        -1,
        false,
        0,
        true,
        -1,
        false,
        -1,
    }

    r.Durable = durable
    r.IsLeader = Isleader

    r.prepareRPC = r.RegisterRPC(new(optgpaxosproto.Prepare), r.prepareChan)
    r.acceptRPC = r.RegisterRPC(new(optgpaxosproto.Accept), r.acceptChan)
    r.fastAcceptRPC = r.RegisterRPC(new(optgpaxosproto.FastAccept), r.fastAcceptChan)
    r.commitRPC = r.RegisterRPC(new(optgpaxosproto.Commit), r.commitChan)
    r.fullCommitRPC = r.RegisterRPC(new(optgpaxosproto.FullCommit), r.fullCommitChan)
    r.prepareReplyRPC = r.RegisterRPC(new(optgpaxosproto.PrepareReply), r.prepareReplyChan)
    r.acceptReplyRPC = r.RegisterRPC(new(optgpaxosproto.AcceptReply), r.acceptReplyChan)
    r.fastAcceptReplyRPC = r.RegisterRPC(new(optgpaxosproto.FastAcceptReply), r.fastAcceptReplyChan)

    go r.run()

    return r
}

//append a log entry to stable storage
func (r *Replica) recordInstanceMetadata(inst *Instance) {
    if !r.Durable {
        return
    }
    var b [5]byte
    binary.LittleEndian.PutUint32(b[0:4], uint32(inst.ballot))
    b[4] = byte(inst.status)
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
    r.IsLeader = true
    log.Println("I am the leader")
    time.Sleep(5 * time.Second) // wait that the connection is actually lost
    r.forceNewBallot = true
    r.Mutex.Unlock()
    return nil
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

    if r.IsLeader {
        log.Println("I am the leader")
    }

    if r.Exec {
        go r.executeCommands()
    }

    clockChan = make(chan bool, 1)
    go r.clock()
    onOffProposeChan := r.ProposeChan

    go r.WaitForClientConnections()

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
            onOffProposeChan = nil
            break

        case prepareS := <-r.prepareChan:
            prepare := prepareS.(*optgpaxosproto.Prepare)
            //got a Prepare message
            dlog.Printf("%d: Received Prepare from replica %d, for instance %d\n", r.Id, prepare.LeaderId, prepare.Instance)
            r.handlePrepare(prepare)
            break

        case acceptS := <-r.acceptChan:
            accept := acceptS.(*optgpaxosproto.Accept)
            //got an Accept message
            dlog.Printf("%d: Received Accept from replica %d, for instance %d\n", r.Id, accept.LeaderId, accept.Instance)
            r.handleAccept(accept)
            break

        case fastAcceptS := <-r.fastAcceptChan:
            fastAccept := fastAcceptS.(*optgpaxosproto.FastAccept)
            //got an Accept message
            dlog.Printf("%d: Received Fast Accept from replica %d, for instance %d with id %d\n", r.Id, fastAccept.LeaderId, fastAccept.Instance, fastAccept.Id)
            r.handleFastAccept(fastAccept)
            break

        case commitS := <-r.commitChan:
            commit := commitS.(*optgpaxosproto.Commit)
            //got a Commit message
            dlog.Printf("%d: Received Commit from replica %d, for instance %d with id %d\n", r.Id, commit.LeaderId, commit.Instance, commit.Id)
            r.handleCommit(commit)
            break

        case fullCommitS := <-r.fullCommitChan:
            fullCommit := fullCommitS.(*optgpaxosproto.FullCommit)
            dlog.Printf("%d: Received Full Commit from replica %d, for instance %d\n", r.Id, fullCommit.LeaderId, fullCommit.Instance)
            r.handleFullCommit(fullCommit)
            break

        case prepareReplyS := <-r.prepareReplyChan:
            prepareReply := prepareReplyS.(*optgpaxosproto.PrepareReply)
            //got a Prepare reply
            dlog.Printf("%d: Received PrepareReply for instance %d\n", r.Id, prepareReply.Instance)
            r.handlePrepareReply(prepareReply)
            break

        case acceptReplyS := <-r.acceptReplyChan:
            acceptReply := acceptReplyS.(*optgpaxosproto.AcceptReply)
            //got an Accept reply
            dlog.Printf("%d: Received AcceptReply for instance %d\n", r.Id, acceptReply.Instance)
            r.handleAcceptReply(acceptReply)
            break

        case fastAcceptReplyS := <-r.fastAcceptReplyChan:
            fastAcceptReply := fastAcceptReplyS.(*optgpaxosproto.FastAcceptReply)
            //got an Accept reply
            dlog.Printf("%d: Received FastAcceptReply for instance %d\n", r.Id, fastAcceptReply.Instance)
            r.handleFastAcceptReply(fastAcceptReply)
        break
        }

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

func (r *Replica) updateCommittedUpTo() {
    for r.instanceSpace[r.committedUpTo+1] != nil{
        if r.instanceSpace[r.committedUpTo+1].status == FINISHED || r.instanceSpace[r.committedUpTo+1].status == FAST_ACCEPT {
            r.committedUpTo++
        }
    }
    dlog.Printf("Committed up to %d", r.committedUpTo)
}



// Gets dependencies from previous instances and current instance.
func (r *Replica) getDeps(cmd state.Command, instNo int32) []state.Id {
    deps := make([]state.Id, 0, len(r.instanceSpace))
    //for i := 0; int32(i) <= instNo; i++ {
        otherInst := r.instanceSpace[instNo]
        if otherInst != nil && otherInst.status != PREPARING {
            for id, cmds := range otherInst.cmds {
                if isConflicting(cmd, cmds) {
                    deps = append(deps, id)
                    break;
                }
            }
        }
    //}
    return deps
}

func (r *Replica) checkDependenciesDelived(id state.Id, instNo int32) bool {
    deps := r.instanceSpace[instNo].deps[id]
    ret := true
    var failed string
    for _,d := range deps {
        other := r.instanceSpace[instNo]
        if other.cmds[d] == nil {
            ret = false
            failed = fmt.Sprintf("Missing %v in instance %d", d, instNo)
            break;
        } else if r.instanceSpace[instNo].status == FAST_ACCEPT && other.phase[d] != DELIVERED {
            ret = false
            failed = fmt.Sprintf("%v FAST ACCEPT but not delivered", d)
            break;
        } else if(other.status == SLOW_ACCEPT ){
            //I think this is not necessary
            ret = false
            failed = fmt.Sprintf("%v SLOW ACCEPT", d)
            break;
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

func (r *Replica) getFullCommands(instance int32) *state.FullCmds {
    inst := r.instanceSpace[instance]
    fullCmds := &state.FullCmds{
        nil,
        nil,
    }

    for id, cmd := range inst.cmds{
        fullCmds.C = append(fullCmds.C, cmd)
        fullCmds.D = append(fullCmds.D, inst.deps[id])
    }

    return fullCmds
}

func (r *Replica) getSeparateCommands(fullCmds *state.FullCmds) (map[state.Id][]state.Command, map[state.Id][]state.Id)  {
    C := make(map[state.Id][]state.Command)
    D := make(map[state.Id][]state.Id)
    return C, D
}

func newInstance(createLb bool) *Instance{
    var lb *LeaderBookkeeping

    if(createLb){
        lb = &LeaderBookkeeping{
            make(map[state.Id][]*genericsmr.Propose),
            0,
            make(map[state.Id]int),
            make(map[state.Id]int),
            0,
            0,
        }
    }

    inst := &Instance{
        make(map[state.Id][]state.Command),
        make(map[state.Id][]state.Id),
        -1,
        make(map[state.Id]Phase),
        -1,
        lb,

    }
    return inst
}

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

func (r *Replica) bcastPrepare(instance int32, ballot int32, toInfinity bool) {
    dlog.Printf("Send 1A message")
    ti := FALSE
    if toInfinity {
        ti = TRUE
    }
    args := &optgpaxosproto.Prepare{r.Id, instance, ballot, ti}

    n := r.N
    if r.Thrifty {
        n = int(math.Ceil(float64(n)/2))
    }

    r.bcast(n, r.prepareRPC, args)

}

func (r *Replica) bcastFastAccept(instance int32, ballot int32, id state.Id, command []state.Command) {
    dlog.Printf("Send 2A Fast message")
    args := &optgpaxosproto.FastAccept{ instance, r.Id, ballot, id, command}

    n := r.N
    if r.Thrifty {
        n = int(math.Ceil(3*float64(n)/4))-1
    }

    r.bcast(n, r.fastAcceptRPC, args)
}


func (r *Replica) bcastAccept(instance int32, ballot int32, command map[state.Id][]state.Command, deps map[state.Id][]state.Id) {
    dlog.Printf("Send 2A message")

    args := &optgpaxosproto.Accept{r.Id,instance,ballot,*r.getFullCommands(instance)}

    n := r.N
    if r.Thrifty {
        n = int(math.Ceil(float64(n)/2))
    }
    r.bcast(n, r.acceptRPC, args)
}

func (r *Replica) bcastCommit(instance int32, ballot int32, id state.Id, command []state.Command, deps []state.Id) {
    dlog.Printf("Send Commit message")
    defer func() {
        if err := recover(); err != nil {
            log.Println("Commit bcast failed:", err)
        }
    }()

    args := &optgpaxosproto.Commit{r.Id,instance,ballot,id,command,deps}

    r.bcast(r.N, r.commitRPC, args)

}

func (r *Replica) bcastFullCommit(instance int32, ballot int32, command map[state.Id][]state.Command, deps map[state.Id][]state.Id) {
    dlog.Printf("Send Full Commit message")
    defer func() {
        if err := recover(); err != nil {
            log.Println("Commit bcast failed:", err)
        }
    }()
    args := &optgpaxosproto.FullCommit{r.Id,instance,ballot,*r.getFullCommands(instance)}

    r.bcast(r.N, r.fullCommitRPC, args)
}

func (r *Replica) handlePropose(propose *genericsmr.Propose) {
    if !r.IsLeader {
        dlog.Printf("Not the leader, cannot propose %v\n", propose.CommandId)
        preply := &genericsmrproto.ProposeReplyTS{FALSE, -1, state.NIL(), 0}
        r.ReplyProposeTS(preply, propose.Reply, propose.Mutex)
        return
    }

    dlog.Printf("Handle propose")

    for r.instanceSpace[r.crtInstance] != nil && r.instanceSpace[r.crtInstance].status != FAST_ACCEPT {
        r.crtInstance++
    }

    instNo := r.crtInstance

    batchSize := len(r.ProposeChan) + 1

    if batchSize > MAX_BATCH {
        batchSize = MAX_BATCH
    }

    dlog.Printf("Batched %d", batchSize)

    //TODO: Check ID of commands with multiple clients.
    cmdId := state.Id(propose.CommandId)
    dlog.Printf("ID for command: %d", cmdId)

    cmds := make([]state.Command, batchSize)
    proposals := make([]*genericsmr.Propose, batchSize)
    cmds[0] = propose.Command
    proposals[0] = propose

    for i := 1; i < batchSize; i++ {
        prop := <-r.ProposeChan
        cmds[i] = propose.Command
        proposals[i] = prop
    }

    ballot := r.makeBallot(instNo)

    if r.defaultBallot != ballot || r.forceNewBallot {
        if r.instanceSpace[instNo] == nil {
            inst := newInstance(true)
            inst.cmds[cmdId] = cmds
            inst.ballot = ballot
            inst.status = PREPARING
            inst.phase[cmdId] = START
            inst.lb.proposalsById[cmdId] = proposals
            r.instanceSpace[instNo] = inst
        }
        r.forceNewBallot = false
        dlog.Printf("Classic round for instance %d (%d,%d)\n", instNo, ballot, r.defaultBallot)
        r.bcastPrepare(instNo, ballot, true)
    } else {

        var deps []state.Id
        for _, cmd := range cmds {
            deps = append(deps, r.getDeps(cmd, instNo)...)
        }

        inst := r.instanceSpace[instNo]
        if inst == nil {
            inst = newInstance(true)
            r.instanceSpace[instNo] = inst
        }
        inst.cmds[cmdId] = cmds
        inst.deps[cmdId] = deps
        inst.ballot = ballot
        inst.status = FAST_ACCEPT
        inst.phase[cmdId] = ACCEPTED
        inst.lb.fastAcceptOKs[cmdId] = 1
        inst.lb.fastAcceptNOKs[cmdId] = 0
        inst.lb.proposalsById[cmdId] = proposals
        dlog.Printf("Created fastAcceptOKs for %d, at instance %d", cmdId, instNo)

        r.recordInstanceMetadata(r.instanceSpace[instNo])
        r.recordCommands(cmds)
        r.sync()
        dlog.Printf("Fast round for instance %d (%d)\n", instNo, ballot)
        r.bcastFastAccept(instNo, ballot, cmdId, cmds)
    }
}

func (r *Replica) handlePrepare(prepare *optgpaxosproto.Prepare) {
    dlog.Printf("Reply to 1A message with 1B message for instance %d with ballot", prepare.Instance, prepare.Ballot)
    inst := r.instanceSpace[prepare.Instance]
    var preply *optgpaxosproto.PrepareReply

    if inst == nil {
        ok := TRUE
        if r.defaultBallot > prepare.Ballot {
            dlog.Printf("Lower than default! %d < %d", prepare.Ballot, r.defaultBallot)
            ok = FALSE
        }
        preply = &optgpaxosproto.PrepareReply{prepare.Instance, ok, r.defaultBallot, state.FullCmds{}}
    } else {
        ok := TRUE
        if prepare.Ballot < inst.ballot {
            dlog.Printf("Lower than last ballot! %d < %d", prepare.Ballot, inst.ballot)
            ok = FALSE
        }
        preply = &optgpaxosproto.PrepareReply{prepare.Instance, ok, inst.ballot, *r.getFullCommands(prepare.Instance)}
    }

    r.replyPrepare(prepare.LeaderId, preply)

    if prepare.ToInfinity == TRUE && prepare.Ballot > r.defaultBallot {
        r.defaultBallot = prepare.Ballot
    }
}

func (r *Replica) handleFastAccept(fastAccept *optgpaxosproto.FastAccept) {
    dlog.Printf("Respond 2A Fast message with 2B Fast message for instance %d with id %d", fastAccept.Instance, fastAccept.Id)
    inst := r.instanceSpace[fastAccept.Instance]

    var areply *optgpaxosproto.FastAcceptReply
    var deps []state.Id

    for _, cmd := range fastAccept.Command {
        deps = append(deps, r.getDeps(cmd, fastAccept.Instance)...)
    }



    if inst == nil {
        if fastAccept.Ballot < r.defaultBallot {
            dlog.Printf("Lower than default! %d < %d", fastAccept.Ballot, r.defaultBallot)
            areply = &optgpaxosproto.FastAcceptReply{fastAccept.Instance, FALSE, r.defaultBallot, -1, nil}
        } else {
            inst = newInstance(false)
            inst.cmds[fastAccept.Id] = fastAccept.Command
            inst.deps[fastAccept.Id] = deps
            inst.ballot = fastAccept.Ballot
            inst.status = FAST_ACCEPT
            inst.phase[fastAccept.Id] = ACCEPTED
            r.instanceSpace[fastAccept.Instance] = inst

            areply = &optgpaxosproto.FastAcceptReply{fastAccept.Instance, TRUE, r.defaultBallot, fastAccept.Id, deps}
        }
    } else if inst.ballot > fastAccept.Ballot {
        dlog.Printf("Lower than current! %d < %d", inst.ballot, r.defaultBallot)
        areply = &optgpaxosproto.FastAcceptReply{fastAccept.Instance, FALSE, inst.ballot, fastAccept.Id, nil}
    } else if inst.ballot < fastAccept.Ballot {
        dlog.Printf("SHOULD NEVER ENTER HERE - Cannot have a higher fast ballot number for the same instance")

    } else {
        // fast path in same instance.
        if r.instanceSpace[fastAccept.Instance].status == FAST_ACCEPT {
            inst.cmds[fastAccept.Id] = fastAccept.Command
            inst.deps[fastAccept.Id] = deps
            inst.phase[fastAccept.Id] = ACCEPTED
            //inst.orderedIds = append(inst.orderedIds, fastAccept.Id)

        } else {
            dlog.Printf("SHOULD NEVER ENTER HERE - Similar reason as before")

        }
        areply = &optgpaxosproto.FastAcceptReply{fastAccept.Instance, TRUE, r.defaultBallot, fastAccept.Id, deps}
    }
    if areply.OK == TRUE {
        r.recordInstanceMetadata(r.instanceSpace[fastAccept.Instance])
        r.recordCommands(fastAccept.Command)
        r.sync()
    }
    dlog.Printf("Going to reply to %d with %v", fastAccept.LeaderId, areply.OK)
    r.replyFastAccept(fastAccept.LeaderId, areply)
}

func (r *Replica) handleAccept(accept *optgpaxosproto.Accept) {
    dlog.Printf("Respond 2A message with 2B message for instance %d with ballot", accept.Instance, accept.Ballot)
    inst := r.instanceSpace[accept.Instance]
    C,D := r.getSeparateCommands(&accept.Cmds)
    var areply *optgpaxosproto.AcceptReply

    if inst == nil {
        if accept.Ballot < r.defaultBallot {
            dlog.Printf("Lower than default! %d < %d", accept.Ballot, r.defaultBallot)
            areply = &optgpaxosproto.AcceptReply{accept.Instance, FALSE, r.defaultBallot}
        } else {
            inst = newInstance(false)
            inst.cmds = C
            inst.deps = D
            inst.ballot = accept.Ballot
            inst.status = SLOW_ACCEPT
            r.instanceSpace[accept.Instance] = inst
            areply = &optgpaxosproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot}

        }
    } else if inst.ballot > accept.Ballot {
        dlog.Printf("Lower than current! %d < %d", inst.ballot, r.defaultBallot)
        areply = &optgpaxosproto.AcceptReply{accept.Instance, FALSE, inst.ballot}
    } else if inst.ballot < accept.Ballot {
        inst.cmds = C
        inst.deps = D
        inst.ballot = accept.Ballot
        inst.status = SLOW_ACCEPT
        areply = &optgpaxosproto.AcceptReply{accept.Instance, TRUE, inst.ballot}
    } else {
        // reordered ACCEPT
        r.instanceSpace[accept.Instance].cmds = C
        if r.instanceSpace[accept.Instance].status != FINISHED {
            r.instanceSpace[accept.Instance].status = SLOW_ACCEPT
        }
        areply = &optgpaxosproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot}
    }

    if areply.OK == TRUE {
        r.recordInstanceMetadata(r.instanceSpace[accept.Instance])
        //TODO: Does not ensure order of commands within same instace. Must use a sequence
        //for _, id:= range S {
        for _, cmds:= range C {
            r.recordCommands(cmds)
        }
        r.sync()
    }

    r.replyAccept(accept.LeaderId, areply)
}

func (r *Replica) handleCommit(commit *optgpaxosproto.Commit) {
    inst := r.instanceSpace[commit.Instance]
    dlog.Printf("Handle commit for instance %d.", commit.Instance)

    if inst == nil {
        inst = newInstance(false)
        inst.cmds[commit.Id] = commit.Command
        inst.deps[commit.Id] = commit.Deps
        inst.ballot = commit.Ballot
        inst.status = FAST_ACCEPT
        inst.phase[commit.Id] = COMMITTED
        r.instanceSpace[commit.Instance] = inst
    } else {
        if(inst.status != FAST_ACCEPT){
            dlog.Printf("Instance moved to slow round.")
        }
        inst.cmds[commit.Id] = commit.Command
        inst.deps[commit.Id] = commit.Deps
        inst.ballot = commit.Ballot
        inst.phase[commit.Id] = COMMITTED
        //Removed inject commands here
    }

    r.updateCommittedUpTo()
    r.recordInstanceMetadata(r.instanceSpace[commit.Instance])
    r.recordCommands(commit.Command)
    dlog.Printf("Pending Commands %d", len(r.ProposeChan))

}

func (r *Replica) handleFullCommit(fullCommit *optgpaxosproto.FullCommit) {
    inst := r.instanceSpace[fullCommit.Instance]
    dlog.Printf("Handle full commit for instance %d with ballot %d", fullCommit.Instance, fullCommit.Ballot)

    C,D:= r.getSeparateCommands(&fullCommit.Cmds)

    if inst == nil {
        inst = newInstance(false)
        inst.cmds = C
        inst.deps = D
        inst.ballot = fullCommit.Ballot
        inst.status = FINISHED
        r.instanceSpace[fullCommit.Instance] = inst

    } else {
        r.instanceSpace[fullCommit.Instance].cmds = C
        r.instanceSpace[fullCommit.Instance].deps = D
        r.instanceSpace[fullCommit.Instance].ballot = fullCommit.Ballot
        r.instanceSpace[fullCommit.Instance].status = FINISHED
        //Removed inject commands here
    }

    r.updateCommittedUpTo()

    r.recordInstanceMetadata(r.instanceSpace[fullCommit.Instance])
    //for _, id := range S{
    for _, cmds:= range C {
        r.recordCommands(cmds)
    }
}



//TODO: How to start a fast ballot?
func (r *Replica) handlePrepareReply(preply *optgpaxosproto.PrepareReply) {
    dlog.Printf("Handle 1B message response")
    inst := r.instanceSpace[preply.Instance]

    if inst.status != PREPARING {
        // TODO: should replies for non-current ballots be ignored?
        // we've moved on -- these are delayed replies, so just ignore
        dlog.Printf("Ignoring instances not in PREPARING stage")
        return
    }

    if preply.OK == TRUE {
        inst.lb.prepareOKs++
        if inst.lb.prepareOKs > r.N>>1 {
            dlog.Printf("Received %d OKs for instance %d. Proceeding", inst.lb.prepareOKs, preply.Instance)
            if inst.ballot > r.defaultBallot {
                r.defaultBallot = inst.ballot
            }
            inst.status = PREPARED
            inst.lb.nacks = 0
            r.recordInstanceMetadata(r.instanceSpace[preply.Instance])
            r.sync()
            //TODO: Maybe decide if it is slow or fast based on the current instance status.
            r.bcastAccept(preply.Instance, inst.ballot, inst.cmds, inst.deps/*, inst.orderedIds*/)
        }
    } else {
        dlog.Printf("There is another active leader (", preply.Ballot, " > ", r.maxRecvBallot, ")")
        inst.lb.nacks++
        if preply.Cmds.C != nil {
            C,D := r.getSeparateCommands(&preply.Cmds)
            inst.cmds = C
            inst.deps = D
        }
        if preply.Ballot > r.maxRecvBallot {
            r.maxRecvBallot = preply.Ballot
        }
        if inst.lb.nacks >= r.N>>1 {
            //Removed inject commands here
            dlog.Printf("What to do here - 4?")
        }
    }
}

func (r *Replica) handleFastAcceptReply(areply *optgpaxosproto.FastAcceptReply) {
    dlog.Printf("Handle 2B Fast response")
    inst := r.instanceSpace[areply.Instance]

    if !r.IsLeader{
        dlog.Printf("Received FastAcceptReply but I am not the leader")
        return;
    }

    if inst.status != PREPARED && inst.status != FAST_ACCEPT{
        // we've move on, these are delayed replies, so just ignore
        return
    }

    if areply.OK == TRUE {
        allEqual := true
        dlog.Printf("Dep vector %d, len: %d", inst.deps[areply.Id], len(inst.deps[areply.Id]))
        if(inst.deps[areply.Id] != nil){
            for i, depi := range areply.Deps {
                if(inst.deps[areply.Id][i] != depi){
                    allEqual = false;
                    break
                }
            }
        }
        if allEqual {
            dlog.Printf("Got same dependency vector for %v, instance %v. Vector size: %d", areply.Id, areply.Instance, len(inst.deps[areply.Id]))
            inst.lb.fastAcceptOKs[areply.Id]++
        }
        if inst.lb.fastAcceptOKs[areply.Id] > int(math.Ceil(3*float64(r.N)/4))-1 {
            inst = r.instanceSpace[areply.Instance]
            inst.phase[areply.Id] = COMMITTED
            r.bcastCommit(areply.Instance, inst.ballot, areply.Id, inst.cmds[areply.Id], inst.deps[areply.Id])
            //TODO: See executeCommands
            if inst.lb.proposalsById[areply.Id] != nil && !r.Dreply {
                // give client the all clear
                for i := 0; i < len(inst.cmds[areply.Id]); i++ {
                    propreply := &genericsmrproto.ProposeReplyTS{
                        TRUE,
                        inst.lb.proposalsById[areply.Id][i].CommandId,
                        state.NIL(),
                        inst.lb.proposalsById[areply.Id][i].Timestamp}
                    r.ReplyProposeTS(propreply, inst.lb.proposalsById[areply.Id][i].Reply, inst.lb.proposalsById[areply.Id][i].Mutex)
                }
            }

            r.recordInstanceMetadata(r.instanceSpace[areply.Instance])
            r.sync() //is this necessary?
            r.updateCommittedUpTo()
        }
    } else {
        dlog.Printf("There is another active leader (", areply.Ballot, " > ", r.maxRecvBallot, ")")
        inst.lb.fastAcceptNOKs[areply.Id]++
        if areply.Ballot > r.maxRecvBallot {
            r.maxRecvBallot = areply.Ballot
        }
        if inst.lb.nacks > r.N >> 1 {
            dlog.Printf("Recovery not implemented")
            // TODO
        }
    }
}

func (r *Replica) handleAcceptReply(areply *optgpaxosproto.AcceptReply) {
    dlog.Printf("Handle 2B response (check can commit)")
    inst := r.instanceSpace[areply.Instance]

    if inst.status != PREPARED && inst.status != SLOW_ACCEPT {
        // we've move on, these are delayed replies, so just ignore
        return
    }

    if areply.OK == TRUE {
        inst.lb.acceptOKs++
        if inst.lb.acceptOKs > r.N>>1 {
            inst = r.instanceSpace[areply.Instance]
            inst.status = FINISHED
            r.bcastFullCommit(areply.Instance, inst.ballot, inst.cmds, inst.deps/*, inst.orderedIds*/)
            //TODO: See executeCommands
            if len(inst.lb.proposalsById) > 0 && !r.Dreply {
                // give client the all clear
                for id, _ := range inst.cmds{
                    for i := 0; i < len(inst.lb.proposalsById[id]); i++ {
                        propreply := &genericsmrproto.ProposeReplyTS{
                            TRUE,
                            inst.lb.proposalsById[id][i].CommandId,
                            state.NIL(),
                            inst.lb.proposalsById[id][i].Timestamp}
                        r.ReplyProposeTS(propreply, inst.lb.proposalsById[id][i].Reply, inst.lb.proposalsById[id][i].Mutex)
                    }
                }
            }

            r.recordInstanceMetadata(r.instanceSpace[areply.Instance])
            r.sync() //is this necessary?
            r.updateCommittedUpTo()
        }
    } else {
        dlog.Printf("There is another active leader (", areply.Ballot, " > ", r.maxRecvBallot, ")")
        // TODO: there is probably another active leader
        inst.lb.nacks++
        if areply.Ballot > r.maxRecvBallot {
            r.maxRecvBallot = areply.Ballot
        }
        if inst.lb.nacks >= r.N>>1 {
            // TODO
        }
    }
}

func (r *Replica) executeCommands() {
    i := int32(0)
    for !r.Shutdown {
        executed := false
        // FIXME idempotence
        for i <= r.committedUpTo {
            if r.instanceSpace[i].cmds != nil {
                inst := r.instanceSpace[i]
                executed = false;
                for id, _ := range inst.cmds {
                    if inst.phase[id] != DELIVERED && r.checkDependenciesDelived(id, i){
                        dlog.Printf("Trying to execute instance: %d, id: %d, status: %v, phase %v", i, id, inst.status, inst.phase[id])
                        for j, cmd := range inst.cmds[id] {
                            if r.Dreply && inst.lb != nil && inst.lb.proposalsById != nil {
                                val := cmd.Execute(r.State)
                                propreply := &genericsmrproto.ProposeReplyTS{
                                    TRUE,
                                    inst.lb.proposalsById[id][j].CommandId,
                                    val,
                                    inst.lb.proposalsById[id][j].Timestamp}
                                r.ReplyProposeTS(propreply, inst.lb.proposalsById[id][j].Reply, inst.lb.proposalsById[id][j].Mutex)
                                dlog.Printf("Delivered %d to client", id)
                            } else if cmd.Op == state.PUT {
                                cmd.Execute(r.State)
                            }
                        }
                        executed = true
                        if inst.status == FAST_ACCEPT{
                            inst.phase[id] = DELIVERED
                            dlog.Printf("Set %d to DELIVERED", id)
                        }
                    }
                }
                if(inst.status == FINISHED){
                    dlog.Printf("Instance %d is finished", i)
                    i++
                }

                if !executed && inst.status == FAST_ACCEPT{
                    break
                }

            } else {
                dlog.Printf("Retrieving instance %d. Going to start prepare", i)
                r.bcastPrepare(i, r.makeBallot(i), false)
                break
            }
        }

        if !executed {
            time.Sleep(1* time.Second)
        }

    }
}