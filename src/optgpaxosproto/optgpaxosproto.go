package optgpaxosproto

import "state"

type FastAccept struct {
    LeaderId int32
    Ballot   int32
    Id		 state.Id
    Cmd  []state.Command
}

type FastAcceptReply struct {
    Ballot   int32
    OK       uint8
    Id	 state.Id
    Cmd  []state.Command
    Deps []state.Id
}

type Accept struct {
    LeaderId int32
    Ballot   int32
    Id       state.Id
    Cmd      []state.Command
    Dep      []state.Id
}

type AcceptReply struct {
    Ballot   int32
    OK       uint8
    Id       state.Id
    Cmd      []state.Command
    Dep      []state.Id
}

type Commit struct {
    LeaderId int32
    Id 		 state.Id
    Cmd      []state.Command
    Deps	 []state.Id
}

type Sync struct {
    LeaderId int32
    Ballot   int32
    Cmds     state.FullCmds
}

type SyncReply struct {
    LeaderId int32
    Ballot   int32
    OK       uint8

}

type NewLeader struct {
    LeaderId int32
    Ballot   int32
}

type NewLeaderReply struct {
    LeaderId int32
    ReplicaId int32
    Ballot   int32
    CBallot  int32
    Cmds     state.FullCmds
}
