package optgpaxosproto

import "state"

type Prepare struct {
	LeaderId   int32
	Instance   int32
	Ballot     int32
	ToInfinity uint8
}

type PrepareReply struct {
    Instance int32
    OK       uint8
    Ballot   int32
    Cmds     state.FullCmds
}

type FastAccept struct {
    Instance int32
    LeaderId int32
    Ballot   int32
    Id		 state.Id
    Command  []state.Command
}

type FastAcceptReply struct {
    Instance int32
    OK       uint8
    Ballot   int32
    Id	 state.Id
    Deps []state.Id
}

type Accept struct {
    LeaderId int32
    Instance int32
    Ballot   int32
    Cmds     state.FullCmds
}

type AcceptReply struct {
    Instance int32
    OK       uint8
    Ballot   int32
}

type Commit struct {
    LeaderId int32
    Instance int32
    Ballot   int32
    Id 		 state.Id
    Command  []state.Command
    Deps	 []state.Id
}

type FullCommit struct {
    LeaderId int32
    Instance int32
    Ballot   int32
    Cmds     state.FullCmds
}
