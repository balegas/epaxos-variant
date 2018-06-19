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
    "sync"
)

//TODO: Add short commit again

//const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)
const MAX_BATCH = 1

type Replica struct {
    *genericsmr.Replica // extends a generic Paxos replica
    acceptChan          chan fastrpc.Serializable
    fastAcceptChan      chan fastrpc.Serializable
    commitChan          chan fastrpc.Serializable
    newLeaderChan       chan fastrpc.Serializable
    syncChan            chan fastrpc.Serializable
    acceptReplyChan     chan fastrpc.Serializable
    fastAcceptReplyChan chan fastrpc.Serializable
    newLeaderReplyChan  chan fastrpc.Serializable
    syncReplyChan       chan fastrpc.Serializable
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
    newLeaderRPC        uint8
    newLeaderReplyRPC   uint8
    status              Status
    maxRecvBallot       int32
    bal                 int32
    cbal                int32
    Shutdown            bool
    flush               bool
    cmds                map[state.Id][]state.Command
    deps                map[state.Id][]state.Id
    phase               map[state.Id]Phase
    lb                  *LeaderBookkeeping
    mutex               *sync.Mutex
    counter             int32
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
    waitingSyncReply
)

type LeaderBookkeeping struct {
    proposalsById       map[state.Id][]*genericsmr.Propose
    prepareOKs          map[state.Id]int
    fastAcceptOKs       map[state.Id]int
    fastAcceptNOKs      map[state.Id]int
    acceptOKs           map[state.Id]int
    acceptNOKs          map[state.Id]int
    newLeaderOKs        map[int32]int
    syncOKs             map[int32]int
    syncNOKs            map[state.Id]int
    cmds                map[state.Id][]state.Command
    deps                map[state.Id]map[int32][]state.Id
    phase               map[state.Id]state.Phase
    count               map[state.Id]int32
    maxBallot           int32
}


type channels struct {
    proposeChan         chan *genericsmr.Propose
    acceptChan          chan fastrpc.Serializable
    fastAcceptChan      chan fastrpc.Serializable
    commitChan          chan fastrpc.Serializable
    newLeaderChan       chan fastrpc.Serializable
    syncChan      chan fastrpc.Serializable
    acceptReplyChan     chan fastrpc.Serializable
    fastAcceptReplyChan chan fastrpc.Serializable
    newLeaderReplyChan       chan fastrpc.Serializable
    syncReplyChan       chan fastrpc.Serializable

}

type connection struct{
    is	*chansmr.ChanReader
    os  *chansmr.ChanWriter
}

func NewReplicaStub(id int, peerAddrList []string, IsLeader bool, thrifty bool, exec bool, lread bool, dreply bool, durable bool, chans *channels, connections map[int32]*connection, control chan bool) *Replica {
    r := &Replica{genericsmr.NewReplica(id, peerAddrList, thrifty, exec, lread, dreply),
        chans.acceptChan,
        chans.fastAcceptChan,
        chans.commitChan,
        chans.newLeaderChan,
        chans.syncChan,
        chans.acceptReplyChan,
        chans.fastAcceptReplyChan,
        chans.newLeaderReplyChan,
        chans.syncReplyChan,
        0, 0, 0,0,0,0,0, 0, 0, 0, 0, 0, 0,
        follower,
        -1,
        -1,
        -1,
        false,
        false,
        make(map[state.Id][]state.Command),
        make(map[state.Id][]state.Id),
        make(map[state.Id]Phase),
        nil,
        new(sync.Mutex),
        0,
    }


    r.Durable = durable
    r.ProposeChan = chans.proposeChan

    if IsLeader {
        r.lb = newLeaderBK()
        r.status = leader
    }

    r.registerRPCs()

    go r.runFake(connections, control)

    return r
}

func NewReplica(id int, peerAddrList []string, isLeader bool, thrifty bool, exec bool, lread bool, dreply bool, durable bool) *Replica {
    r := &Replica{
        Replica:             genericsmr.NewReplica(id, peerAddrList, thrifty, exec, lread, dreply),
        acceptChan:          make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        fastAcceptChan:      make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        commitChan:          make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        newLeaderChan:       make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        syncChan:            make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        acceptReplyChan:     make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        fastAcceptReplyChan: make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        newLeaderReplyChan:  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        syncReplyChan:       make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        prepareRPC:          0,
        acceptRPC:           0,
        fastAcceptRPC:       0,
        commitRPC:           0,
        syncRPC:             0,
        fullCommitRPC:       0,
        commitShortRPC:      0,
        prepareReplyRPC:     0,
        acceptReplyRPC:      0,
        fastAcceptReplyRPC:  0,
        syncReplyRPC:        0,
        newLeaderRPC:        0,
        newLeaderReplyRPC:   0,
        status:              follower,
        maxRecvBallot:       -1,
        bal:                 -1,
        cbal:                -1,
        Shutdown:            false,
        flush:               false,
        cmds:                make(map[state.Id][]state.Command),
        deps:                make(map[state.Id][]state.Id),
        phase:               make(map[state.Id]Phase),
        lb:                  nil,
        mutex:               new(sync.Mutex),
        counter:             0,
    }

    r.Durable = durable
    if isLeader {
        r.lb = newLeaderBK()
        r.status = leader
    }

   r.registerRPCs()

   go r.run()

   return r
}

func (r *Replica) registerRPCs(){
    r.acceptRPC             = r.RegisterRPC(new(optgpaxosproto.Accept), r.acceptChan)
    r.fastAcceptRPC         = r.RegisterRPC(new(optgpaxosproto.FastAccept), r.fastAcceptChan)
    r.commitRPC             = r.RegisterRPC(new(optgpaxosproto.Commit), r.commitChan)
    r.newLeaderRPC          = r.RegisterRPC(new(optgpaxosproto.NewLeader), r.newLeaderChan)
    r.syncRPC               = r.RegisterRPC(new(optgpaxosproto.Sync), r.syncChan)
    r.acceptReplyRPC        = r.RegisterRPC(new(optgpaxosproto.AcceptReply), r.acceptReplyChan)
    r.fastAcceptReplyRPC    = r.RegisterRPC(new(optgpaxosproto.FastAcceptReply), r.fastAcceptReplyChan)
    r.newLeaderReplyRPC     = r.RegisterRPC(new(optgpaxosproto.NewLeaderReply), r.newLeaderReplyChan)
    r.syncReplyRPC          = r.RegisterRPC(new(optgpaxosproto.SyncReply), r.syncReplyChan)
}

func newLeaderBK() *LeaderBookkeeping{
    return &LeaderBookkeeping{
        make(map[state.Id][]*genericsmr.Propose),
        make(map[state.Id]int),
        make(map[state.Id]int),
        make(map[state.Id]int),
        make(map[state.Id]int),
        make(map[state.Id]int),
        make(map[int32]int),
        make(map[int32]int),
        make(map[state.Id]int),
        make(map[state.Id][]state.Command),
        make(map[state.Id]map[int32][]state.Id),
        make(map[state.Id]state.Phase),
        make(map[state.Id]int32),
        -1,
    }
}

//append a log entry to stable storage
//TODO: This must be reviewed.
func (r *Replica) recordInstanceMetadata(id state.Id) {
    if !r.Durable {
        return
    }
    var b [13]byte
    binary.LittleEndian.PutUint32(b[0:4], uint32(r.bal))
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
    //r.Mutex.Lock()
    r.status = preparing
    r.lb = newLeaderBK()

    log.Printf("%d: I am the leader", r.Id)
    time.Sleep(5 * time.Second) // wait that the connection is actually lost
    //r.Mutex.Unlock()

    r.bcastNewLeader()
    return nil
}

func (r *Replica) revokeLeader(){
    log.Println("No longer leader")
    r.status = follower
    r.lb = nil
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

func (r *Replica) replyNewLeader(replicaId int32, reply *optgpaxosproto.NewLeaderReply) {
    r.SendMsg(replicaId, r.newLeaderReplyRPC, reply)
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
        r.mutex.Lock()
        select {

        case <-clockChan:
            //activate the new proposals channel
            onOffProposeChan = r.ProposeChan
            break

        case propose := <-onOffProposeChan:
            //got a Propose from a client
            dlog.Printf("%d: Received Proposal with type=%d\n", r.Id, propose.Command.Op)
            r.handlePropose(propose)
            sendControl(control)
            //deactivate the new proposals channel to prioritize the handling of protocol messages
            onOffProposeChan = nil
            break

        case acceptC := <-r.acceptChan:
            accept := acceptC.(*optgpaxosproto.Accept)
            //got an Accept message
            dlog.Printf("%d: Received Accept from replica %d, for ballot %d with id %d\n", r.Id, accept.LeaderId, accept.Ballot, accept.Id)
            r.handleAccept(accept)
            sendControl(control)
            break

        case fastAcceptC := <-r.fastAcceptChan:
            fastAccept := fastAcceptC.(*optgpaxosproto.FastAccept)
            //got an Accept message
            dlog.Printf("%d: Received FastAccept from replica %d, for ballot %d with id %d\n", r.Id, fastAccept.LeaderId, fastAccept.Ballot, fastAccept.Id)
            r.handleFastAccept(fastAccept)
            sendControl(control)
            break

        case commitC := <-r.commitChan:
            commit := commitC.(*optgpaxosproto.Commit)
            //got a Commit message
            dlog.Printf("%d: Received Commit from replica %d, with id %d\n", r.Id, commit.LeaderId, commit.Id)
            r.handleCommit(commit)
            sendControl(control)
            break

        case newLeaderC := <-r.newLeaderChan:
            newLeader := newLeaderC.(*optgpaxosproto.NewLeader)
            //got a Commit message
            dlog.Printf("%d: Received NewLeader from replica %d, with ballot %d\n", r.Id, newLeader.LeaderId, newLeader.Ballot)
            r.handleNewLeader(newLeader)
            sendControl(control)
            break

        case syncC := <-r.syncChan:
            sync := syncC.(*optgpaxosproto.Sync)
            dlog.Printf("%d: Received Sync from replica %d, for ballot %d\n", r.Id, sync.LeaderId, sync.Ballot)
            r.handleSync(sync)
            sendControl(control)
            break

        case acceptReplyC := <-r.acceptReplyChan:
            acceptReply := acceptReplyC.(*optgpaxosproto.AcceptReply)
            //got an Accept reply
            dlog.Printf("%d: Received AcceptReply for ballot %d\n", r.Id, acceptReply.Ballot)
            r.handleAcceptReply(acceptReply)
            sendControl(control)
            break

        case fastAcceptReplyC := <-r.fastAcceptReplyChan:
            fastAcceptReply := fastAcceptReplyC.(*optgpaxosproto.FastAcceptReply)
            //got an Accept reply
            dlog.Printf("%d: Received FastAcceptReply for ballot %d\n", r.Id, fastAcceptReply.Ballot)
            r.handleFastAcceptReply(fastAcceptReply)
            sendControl(control)
            break

        case newLeaderReplyC := <-r.newLeaderReplyChan:
            newLeaderReply := newLeaderReplyC.(*optgpaxosproto.NewLeaderReply)
            //got a Commit message
            dlog.Printf("%d: Received NewLeaderReply for leader %d with ballot %d\n", r.Id, newLeaderReply.LeaderId, newLeaderReply.Ballot)
            r.handleNewLeaderReply(newLeaderReply)
            sendControl(control)
            break

        case syncReplyC := <-r.syncReplyChan:
            syncReply := syncReplyC.(*optgpaxosproto.SyncReply)
            //got a Prepare reply
            dlog.Printf("%d: Received SyncReply for ballot %d\n", r.Id, syncReply.Ballot)
            r.handleSyncReply(syncReply)
            sendControl(control)
            break

        }
        r.mutex.Unlock()
    }
}

func sendControl(control chan bool){
    //log.Printf("Sent Control")
    if control !=  nil{
        control <- true
    }
}

func (r *Replica) makeBallot(forceNew bool) int32 {
    //log.Printf("Make Ballot orig: %d", r.bal)
    if r.bal == -1 {
        r.bal = r.Id
    }

    if r.maxRecvBallot > r.bal {
        r.bal = r.maxRecvBallot + 1
    } else if forceNew{
        r.bal++
    }
    //log.Printf("Make Ballot ret: %d", r.bal)
    return r.bal
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
//Splitting commands over multiple paxos instances could reduces the cost of this step.
func (r *Replica) getDeps(cmd state.Command) []state.Id {
    deps := make([]state.Id, 0)
    for id, other := range r.cmds {
        if isConflicting(cmd, other) && r.phase[id] != start {
            deps = append(deps, id)
        }
    }
    return deps
}

func (r *Replica) checkDependenciesDelivered(id state.Id, instNo int32) bool {
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


//TODO store dependencies ordered to improve performance
func depsEqual(first, second []state.Id) bool{
    allEqual := true
    if len(first) != len(second) {
        allEqual = false
    } else {
        for _, depi := range first {
            found := false;
            for _, depj := range second {
                if depi == depj {
                    found = true
                    break;
                }
            }

            if !found{
                allEqual = false
                break
            }
        }
    }
    return allEqual
}

//TODO: Please review this
func isConflicting(cmd state.Command, otherCmds []state.Command) bool {
    for _, other := range otherCmds {
        if cmd.K == other.K && (cmd.Op == state.PUT || other.Op == state.PUT) {
            return true
        }
    }
    return false
}

func getFullCommands(cmds map[state.Id][]state.Command,deps map[state.Id][]state.Id, phase map[state.Id]Phase) *state.FullCmds {
    fullCmds := &state.FullCmds{
        Id: make([]state.Id, len(cmds)),
        C:  make([][]state.Command, len(cmds)),
        D:  make([][]state.Id, len(cmds)),
        P:  make([]state.Phase, len(cmds)),
    }

    i := 0
    for id, cmd := range cmds{
        fullCmds.Id[i] = id
        fullCmds.C[i] = cmd
        fullCmds.D[i] = deps[id]
        fullCmds.P[i] = state.Phase(phase[id])
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

func (r *Replica) bcastFastAccept(ballot int32, id state.Id, cmd []state.Command) {
    args := &optgpaxosproto.FastAccept{ LeaderId: r.Id, Ballot: ballot, Id: id, Cmd: cmd}

    n := r.N
    if r.Thrifty {
        n = int(math.Ceil(3*float64(n)/4))
    }

    r.bcast(n-1, r.fastAcceptRPC, args)
}


func (r *Replica) bcastAccept(ballot int32, id state.Id, cmd []state.Command, deps []state.Id) {
    args := &optgpaxosproto.Accept{LeaderId: r.Id, Ballot: ballot, Id: id, Cmd: cmd, Dep: deps}

    n := r.N
    if r.Thrifty {
        n = int(math.Ceil(float64(n)/2))
    }
    r.bcast(n-1, r.acceptRPC, args)
}

func (r *Replica) bcastCommit(ballot int32, id state.Id, cmd []state.Command, deps []state.Id) {
    args := &optgpaxosproto.Commit{LeaderId: r.Id, Id: id, Cmd: cmd, Deps: deps}

    n := r.N
    //if r.Thrifty {
    //    n = int(math.Ceil(float64(n)/2))
    //}

    r.bcast(n-1, r.commitRPC, args)

}

func (r *Replica) bcastNewLeader() {
    args := &optgpaxosproto.NewLeader{r.Id, r.makeBallot(true)}

    n := r.N

    r.bcast(n-1, r.fastAcceptRPC, args)
}

func (r *Replica) bcastSync(ballot int32) {
    args := &optgpaxosproto.Sync{LeaderId: r.Id, Ballot: ballot, Cmds: *getFullCommands(r.cmds, r.deps, r.phase)}

    n := r.N
    //TODO: How many msgs are necessary?
    //if r.Thrifty {
    //    n = int(math.Ceil(float64(n)/2))
    //}

    r.bcast(n-1, r.newLeaderReplyRPC, args)
}


func (r *Replica) handleNewLeader(newLeader *optgpaxosproto.NewLeader){
    var nlreply *optgpaxosproto.NewLeaderReply
    if(r.bal >= newLeader.Ballot ){
        nlreply = &optgpaxosproto.NewLeaderReply{newLeader.LeaderId, r.Id, r.bal, r.cbal, *getFullCommands(r.cmds, r.deps, r.phase)}
    } else{
        r.maxRecvBallot = max(newLeader.Ballot, r.maxRecvBallot)
        r.status = preparing
        r.bal = newLeader.Ballot
        nlreply = &optgpaxosproto.NewLeaderReply{newLeader.LeaderId, r.Id, newLeader.Ballot, r.cbal, *getFullCommands(r.cmds, r.deps, r.phase)}
    }


    r.replyNewLeader(newLeader.LeaderId, nlreply)

}

func (r *Replica) handleNewLeaderReply(nlReply *optgpaxosproto.NewLeaderReply){
    if(r.status != preparing || r.bal != nlReply.Ballot ){
        if(r.bal < nlReply.Ballot){
            r.revokeLeader()
        }
        return
    }

    r.lb.newLeaderOKs[r.bal]++

    C,D,P := r.getSeparateCommands(&nlReply.Cmds)

    if(nlReply.CBallot > r.lb.maxBallot){
        r.lb.cmds = make(map[state.Id][]state.Command)
        r.lb.deps = make(map[state.Id]map[int32][]state.Id)
        r.lb.phase = make(map[state.Id]state.Phase)
        r.lb.maxBallot = nlReply.CBallot
    }

    if(nlReply.CBallot == r.lb.maxBallot ){
        for id, c := range C {
            r.lb.cmds[id] = c
            r.lb.deps[id][nlReply.ReplicaId] = D[id]
            if state.Phase(P[id]) > r.lb.phase[id]{
                r.lb.phase[id] = state.Phase(P[id])
            }
        }
    }

    if r.lb.newLeaderOKs[r.bal] > int(math.Ceil(1*float64(r.N)/2))-1 {
        log.Printf("Sync STATE")

        //change state to avoid late messagess
        r.status = waitingSyncReply
        r.lb.syncOKs[r.bal]++
        r.syncState()
        r.bcastSync(r.bal)
    }

}

func (r *Replica) syncState(){
    syncCmds := make(map[state.Id][]state.Command)
    syncDeps := make(map[state.Id][]state.Id)
    syncPhase := make(map[state.Id]Phase)

    //Add committed to bookKeeping
    for id, c := range r.cmds{
        if(r.phase[id] == committed){
            syncCmds[id] = c
            syncDeps[id] = r.deps[id]
            syncPhase[id] = r.phase[id]
        }
    }

    for id, c := range r.lb.cmds{
        if(r.lb.phase[id] == state.Phase(slowAcceptMode) && syncPhase[id] != committed){
            syncCmds[id] = c
            syncDeps[id] = r.deps[id]
            syncPhase[id] = r.phase[id]
        } else{
            //optimize
            for _, d := range r.lb.deps[id]{
                count := 0
                for _, oD := range r.lb.deps[id]{
                    equal := depsEqual(d, oD)
                    if equal{
                        count++
                    }
                    if (count > int(math.Ceil(1*float64(r.N)/4))){
                        syncCmds[id] = c
                        syncDeps[id] = d
                        syncPhase[id] = slowAcceptMode
                        break
                    }
                }
            }
        }
    }

    for _, deps := range syncDeps {
        for _, d := range deps{
            if(syncPhase[d] == start){
                syncCmds[d] = state.NOOP()
                syncPhase[d] = slowAcceptMode
            }
        }
    }

    r.cmds = syncCmds
    r.deps = syncDeps
    r.phase = syncPhase

    r.lb.cmds = nil
    r.lb.deps = nil
    r.lb.phase = nil
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

    //TODO: propose.CommandId is not unique. Using seq id (non-idempotent).
    //id := state.Id(propose.CommandId)
    id := state.Id(r.counter)
    r.counter++
    dlog.Printf("ID for command: %d for command %+v", id, propose.Command)

    //Replies with NOK if command ID is repeated
    if(r.phase[id] != start){
       dlog.Printf("REPEATED ID")
       if(r.lb.proposalsById[id] != nil){
           for _, p := range r.lb.proposalsById[id] {
               r.deliverCommand(p, FALSE, state.NIL())
           }
       }
       return
    }

    cmds := make([]state.Command, batchSize)
    proposals := make([]*genericsmr.Propose, batchSize)
    cmds[0] = propose.Command
    proposals[0] = propose

    for i := 1; i < batchSize; i++ {
        prop := <-r.ProposeChan
        cmds[i] = propose.Command
        proposals[i] = prop
    }

    ballot := r.makeBallot(false)

    r.lb.proposalsById[id] = proposals

    dlog.Printf("Fast round for cmd id: %d, with ballot %d\n", id, ballot)

    // Leader fast accept -- This code can be same function that the acceptor execute + leaderBookKeeping
    // Code simplification could be applied to all methods
    var deps []state.Id
    for _, cmd := range cmds {
        deps = append(deps, r.getDeps(cmd)...)
    }
    r.phase[id] = fastAcceptMode
    r.cmds[id] = cmds
    r.deps[id] = deps

    r.lb.fastAcceptOKs[id] = 1
    r.lb.fastAcceptNOKs[id] = 0
    r.lb.acceptOKs[id] = 0
    r.lb.acceptNOKs[id] = 0


    //TODO: DID NOT CHECK WHEN TO RECORD TO DISK. Must also review method implementation.
    r.recordInstanceMetadata(id)
    r.recordCommands(r.cmds[id])
    r.sync()

    r.bcastFastAccept(ballot, id, cmds)

}

func (r *Replica) handleFastAccept(fastAccept *optgpaxosproto.FastAccept) {
    if r.status == leader && r.cmds[fastAccept.Id] != nil {
        dlog.Printf("I am the leader for this command, I should not handle FAST Accept: %v\n", fastAccept.Id)
        return
    }
    var fareply *optgpaxosproto.FastAcceptReply
    r.maxRecvBallot = max(fastAccept.Ballot, r.maxRecvBallot)
    id := fastAccept.Id

    if fastAccept.Ballot < r.bal{
        fareply = &optgpaxosproto.FastAcceptReply{Ballot: r.bal, OK: FALSE, Id: fastAccept.Id}
    } else {
        // FAST FORWARD if fastAccept.ballot > r.currBallot. Should handle it differently?
        if r.phase[id] == start {
            var deps []state.Id
            for _, cmd := range fastAccept.Cmd {
                deps = append(deps, r.getDeps(cmd)...)
            }
            r.phase[id] = fastAcceptMode
            r.cmds[id] = fastAccept.Cmd
            r.deps[id] = deps
            r.bal = fastAccept.Ballot
            fareply = &optgpaxosproto.FastAcceptReply{Ballot: r.bal, OK: TRUE, Id: id, Cmd: r.cmds[id], Deps: r.deps[id]}
        } else {
            dlog.Printf("Id has already started.")
            fareply = &optgpaxosproto.FastAcceptReply{Ballot: r.bal, OK: FALSE, Id: id, Deps: nil}
        }

    }

    if fareply.OK == TRUE {
        r.recordInstanceMetadata(id)
        r.recordCommands(fastAccept.Cmd)
        r.sync()
    }

    //FAKE REORDER --- FOR TESTING
    //if fareply.Id == 5 && r.status == follower && (r.Id == 1 || r.Id == 2 || r.Id == 3 || r.Id == 4 || r.Id == 5) {
    //    dlog.Printf("FAKE REORDER OF MESSAGES")
    //    tmpDeps := make([]state.Id, len(fareply.Deps)-1)
    //    tmpDeps =  fareply.Deps[:(len(fareply.Deps) - 1)]
    //    fareply.Deps = tmpDeps
    //}

    //dlog.Printf("%d Response for %d :  %d, %+v", r.Id, fastAccept.Id, fareply.OK, fareply.Deps)
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

    if accept.Ballot < r.bal{
        areply = &optgpaxosproto.AcceptReply{Ballot: r.bal, OK: FALSE, Id: accept.Id}
    } else {
        // FAST FORWARD if fastAccept.ballot > r.currBallot. Should we handle it differently?
        var deps []state.Id
        r.cmds[id] = accept.Cmd
        r.deps[id] = deps
        r.phase[id] = slowAcceptMode
        r.bal = accept.Ballot
        areply = &optgpaxosproto.AcceptReply{Ballot: r.bal, OK: TRUE, Id: id, Cmd: r.cmds[id], Dep: r.deps[id]}


    }

    if areply.OK == TRUE {
        r.recordInstanceMetadata(id)
        r.recordCommands(accept.Cmd)
        r.sync()
        r.replyAccept(accept.LeaderId, areply)
    }

    //Not replying if NOK
}

func (r *Replica) handleCommit(commit *optgpaxosproto.Commit) {
    id := commit.Id
    r.cmds[id] = commit.Cmd
    r.deps[id] = commit.Deps
    r.phase[id] = committed

    r.recordInstanceMetadata(id)
    r.recordCommands(r.cmds[id])

}

func (r *Replica) handleSync(sync *optgpaxosproto.Sync) {

    C,D,P := r.getSeparateCommands(&sync.Cmds)
    r.maxRecvBallot = max(sync.Ballot, r.maxRecvBallot)

    if sync.Ballot == r.bal{
        r.status = follower
        r.bal = sync.Ballot
        r.cbal = sync.Ballot
        r.cmds = C
        r.deps = D
        r.phase = P

        for id := range C {
            r.recordInstanceMetadata(id)
            r.recordCommands(r.cmds[id])
        }

        //TODO: OK parameter is not necessary
        sreply := &optgpaxosproto.SyncReply{LeaderId: sync.LeaderId, OK: TRUE, Ballot: r.bal}
        r.replySync(sync.LeaderId, sreply)
    } else{

    }
}

func (r *Replica) handleSyncReply(sreply *optgpaxosproto.SyncReply) {

    r.maxRecvBallot = max(sreply.Ballot, r.maxRecvBallot)

    if r.status != waitingSyncReply  || sreply.Ballot != r.bal {
        // TODO: should replies for non-current ballots be ignored?
        // we've moved on -- these are delayed replies, so just ignore
        dlog.Printf("Ignoring instances not in PREPARING stage")
        return
    }

    if sreply.OK == TRUE {
        r.lb.syncOKs[r.bal]++

        if r.lb.syncOKs[r.bal] > int(math.Ceil(1*float64(r.N)/2))-1 {
            dlog.Printf("%d: I am the elected leader", r.Id)
            r.status = leader
            for id, _ := range r.phase{
                if(r.phase[id] != committed){
                    r.phase[id] = committed
                    r.bcastCommit(r.bal, id, r.cmds[id], r.deps[id])
                }
            }

        }
    } else {
        if(r.maxRecvBallot > r.bal){
            dlog.Printf("There is another active leader ( %d > %d)", r.maxRecvBallot, r.bal, ")")
            //TODO: Must test leader change.
            if(r.status == leader){
                r.revokeLeader()
            }
        }
    }

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

    if areply.OK == TRUE && r.bal == areply.Ballot {
        allEqual := depsEqual(r.deps[id], areply.Deps)

        if allEqual {
            r.lb.fastAcceptOKs[id]++
            //dlog.Printf("Min OKs > %d, got: %d", int(math.Ceil(3*float64(r.N)/4))-1, r.lb.fastAcceptOKs[id])

        } else{
            r.lb.fastAcceptNOKs[id]++
            //dlog.Printf("Max NOKs : %d, got: %d", int(math.Ceil(1*float64(r.N)/4))-1, r.lb.fastAcceptNOKs[id])
        }
        if r.lb.fastAcceptOKs[id] > int(math.Ceil(3*float64(r.N)/4))-1 {
            r.phase[id] = committed
            r.bcastCommit(r.bal, id, r.cmds[id], r.deps[id])

            //TODO: I am not sure if the code for executing the command immediately is correct.
            if r.lb.proposalsById[id] != nil && !r.Dreply {
                r.tryToDeliverOrExecute(!r.Dreply, false)
            }

            r.recordInstanceMetadata(id)
            r.sync() //is this necessary?
        } else if r.lb.fastAcceptNOKs[areply.Id] > int(math.Ceil(1*float64(r.N)/4)-1){
            dlog.Printf("Collision recovery: set deps for %d: %+v",areply.Id, r.deps[areply.Id])
            r.phase[id] = slowAcceptMode
            r.lb.acceptOKs[id] = 1
            r.lb.acceptNOKs[id] = 0
            r.recordInstanceMetadata(id)
            r.sync()
            r.bcastAccept(r.bal, id, r.cmds[id], r.deps[id])
        }
    } else {
        if(r.maxRecvBallot > r.bal){
            dlog.Printf("There is another active leader ( %d > %d)", r.maxRecvBallot, r.bal, ")")
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

    if areply.OK == TRUE && r.bal == areply.Ballot {
        r.lb.acceptOKs[id]++
        //TODO: What is the size of this quorum?
        if r.lb.acceptOKs[id] > r.N>>1 {
            dlog.Printf("Moved to commit after Recovery")
            r.phase[id] = committed
            r.bcastCommit(r.bal, id, r.cmds[id], r.deps[id])

            if r.lb.proposalsById[id] != nil && !r.Dreply {
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
        r.mutex.Lock()
        r.dumpDeliveryState()
        /*executed := */r.tryToDeliverOrExecute(r.Dreply && r.status == leader, true)
        r.mutex.Unlock()
        //Delivers all possible, so sleep everytime
        //if !executed {
            time.Sleep(1* time.Second)
        //}

    }

}

func (r *Replica) tryToDeliverOrExecute(deliver bool, execute bool) bool{
    //TODO: IMPORTANT: Very unefficient code.
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
                    if !deliver && r.status != leader{
                        r.phase[id] = delivered
                    }
                    if deliver || execute {
                        for i, c := range r.cmds[id]{
                            val := state.NIL()
                            if execute {
                                //dlog.Printf("Trying to execute command with id %d", id)
                                val = c.Execute(r.State)
                            }
                            if deliver {
                                //dlog.Printf("Trying to deliver command with id %d", id)
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

func (r *Replica) dumpDeliveryState(){
    committedArray := make([]state.Id, 0)
    readyArray := make([]state.Id, 0)

    for id := range r.cmds {
        if r.phase[id] == committed {
            committedArray = append(committedArray, id)
        }
        if r.phase[id] == forDelivery {
            readyArray = append(readyArray, id)
        }
    }

    if(len(committedArray) == 0 && len(readyArray) == 0){
        //fmt.Printf("No commands waiting for delivery\n")
    } else{
        if len(committedArray) > 0 {
            //for _, id := range committedArray{
                //fmt.Printf("CommittedArray: %d:%v\n", id, r.deps[id])
            //}

        }
        if len(readyArray) > 0 {
            for _, id := range readyArray{
                fmt.Printf("readyArray: %d:%v\n", id, r.deps[id])
            }

        }
    }
}



func max(x, y int32) int32 {
    if x > y {
        return x
    }
    return y
}

