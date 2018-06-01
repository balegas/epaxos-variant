package optgpaxosproto

import "state"

type Prepare struct {
	LeaderId   int32
	Ballot     int32
	ToInfinity uint8
}

type PrepareReply struct {
    Ballot   int32
    OK       uint8
    Cmds     state.FullCmds
}

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
