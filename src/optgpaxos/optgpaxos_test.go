package optgpaxos

import (
    "testing"
    "fastrpc"
    "chansmr"
    "time"
    "genericsmr"
    "sync"
    "bufio"
    "genericsmrproto"
    "state"
    "math/rand"
    "optgpaxosproto"
)


type RepChan struct{
    C *channels
    R *Replica
}

var connections map[int32]*connection

func initReplicas(count int) []*RepChan{
    ret := make([]*RepChan, count)
    isLeader := true

    connections = make(map[int32]*connection)

    for i := 0 ; i < count; i++ {
        ic := make(chan byte, 4096)
        oc := ic
        connections[int32(i)] = &connection{chansmr.NewChanReader(ic), chansmr.NewChanWriter(oc)}
    }

    for i := 0 ; i < count; i++ {
        repiChan := &channels{
            make(chan *genericsmr.Propose),
            make(chan fastrpc.Serializable),
            make(chan fastrpc.Serializable),
            make(chan fastrpc.Serializable),
            make(chan fastrpc.Serializable),
            make(chan fastrpc.Serializable),
            make(chan fastrpc.Serializable),
            make(chan fastrpc.Serializable),
            make(chan fastrpc.Serializable),
        }

        ret[i] = &RepChan{
            repiChan,
            NewReplicaStub(i, make([]string, count), isLeader, true, false, true, false, false, repiChan, connections),
        }

        isLeader = false
    }
    return ret
}

func TestPropose(t *testing.T) {
    reps := initReplicas(3)

    mutex := &sync.Mutex{}

    proposeChanR := bufio.NewReader(chansmr.NewChanReader(make(chan byte)))
    proposeChanW := bufio.NewWriter(chansmr.NewChanWriter(make(chan byte)))

    io := bufio.NewReadWriter(proposeChanR, proposeChanW)

    value := make([]byte, 4)
    rand.Read(value)

    propose := &genericsmrproto.Propose{0,state.Command{state.PUT, 0, value},0}

    time.Sleep(time.Second * 1)

    reps[0].C.proposeChan <- &genericsmr.Propose{propose, io.Writer, mutex}

    time.Sleep(time.Millisecond * 500)

    connections[1].is.Read(make([]byte, 1))
    prep := new(optgpaxosproto.Prepare)
    prep .Unmarshal(connections[1].is)

    t.Logf("Prep: %v", prep)


}

