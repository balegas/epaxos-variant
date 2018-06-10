package optgpaxosproto

import (
	"bufio"
	"encoding/binary"
	"fastrpc"
	"io"
	"state"
	"sync"
)

type byteReader interface {
	io.Reader
	ReadByte() (c byte, err error)
}

func (t *Accept) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type AcceptCache struct {
	mu    sync.Mutex
	cache []*Accept
}

func NewAcceptCache() *AcceptCache {
	c := &AcceptCache{}
	c.cache = make([]*Accept, 0)
	return c
}

func (p *AcceptCache) Get() *Accept {
	var t *Accept
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Accept{}
	}
	return t
}
func (p *AcceptCache) Put(t *Accept) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Accept) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Id.Marshal(wire)
	bs = b[:]
	alen1 := int64(len(t.Cmd))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Cmd[i].Marshal(wire)
	}
	bs = b[:]
	alen2 := int64(len(t.Dep))
	if wlen := binary.PutVarint(bs, alen2); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen2; i++ {
		t.Dep[i].Marshal(wire)
	}
}

func (t *Accept) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [10]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Id.Unmarshal(wire)
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Cmd = make([]state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Cmd[i].Unmarshal(wire)
	}
	alen2, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Dep = make([]state.Id, alen2)
	for i := int64(0); i < alen2; i++ {
		t.Dep[i].Unmarshal(wire)
	}
	return nil
}

func (t *Sync) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type SyncCache struct {
	mu    sync.Mutex
	cache []*Sync
}

func NewSyncCache() *SyncCache {
	c := &SyncCache{}
	c.cache = make([]*Sync, 0)
	return c
}

func (p *SyncCache) Get() *Sync {
	var t *Sync
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Sync{}
	}
	return t
}
func (p *SyncCache) Put(t *Sync) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Sync) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Cmds.Marshal(wire)
}

func (t *Sync) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Cmds.Unmarshal(wire)
	return nil
}

func (t *NewLeaderReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type NewLeaderReplyCache struct {
	mu    sync.Mutex
	cache []*NewLeaderReply
}

func NewNewLeaderReplyCache() *NewLeaderReplyCache {
	c := &NewLeaderReplyCache{}
	c.cache = make([]*NewLeaderReply, 0)
	return c
}

func (p *NewLeaderReplyCache) Get() *NewLeaderReply {
	var t *NewLeaderReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &NewLeaderReply{}
	}
	return t
}
func (p *NewLeaderReplyCache) Put(t *NewLeaderReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *NewLeaderReply) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.ReplicaId
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = t.CBallot
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Cmds.Marshal(wire)
}

func (t *NewLeaderReply) Unmarshal(wire io.Reader) error {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.ReplicaId = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Ballot = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.CBallot = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	t.Cmds.Unmarshal(wire)
	return nil
}

func (t *NewLeader) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

type NewLeaderCache struct {
	mu    sync.Mutex
	cache []*NewLeader
}

func NewNewLeaderCache() *NewLeaderCache {
	c := &NewLeaderCache{}
	c.cache = make([]*NewLeader, 0)
	return c
}

func (p *NewLeaderCache) Get() *NewLeader {
	var t *NewLeader
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &NewLeader{}
	}
	return t
}
func (p *NewLeaderCache) Put(t *NewLeader) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *NewLeader) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *NewLeader) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	return nil
}

func (t *FastAccept) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type FastAcceptCache struct {
	mu    sync.Mutex
	cache []*FastAccept
}

func NewFastAcceptCache() *FastAcceptCache {
	c := &FastAcceptCache{}
	c.cache = make([]*FastAccept, 0)
	return c
}

func (p *FastAcceptCache) Get() *FastAccept {
	var t *FastAccept
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &FastAccept{}
	}
	return t
}
func (p *FastAcceptCache) Put(t *FastAccept) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *FastAccept) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Id.Marshal(wire)
	bs = b[:]
	alen1 := int64(len(t.Cmd))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Cmd[i].Marshal(wire)
	}
}

func (t *FastAccept) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [10]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Id.Unmarshal(wire)
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Cmd = make([]state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Cmd[i].Unmarshal(wire)
	}
	return nil
}

func (t *FastAcceptReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type FastAcceptReplyCache struct {
	mu    sync.Mutex
	cache []*FastAcceptReply
}

func NewFastAcceptReplyCache() *FastAcceptReplyCache {
	c := &FastAcceptReplyCache{}
	c.cache = make([]*FastAcceptReply, 0)
	return c
}

func (p *FastAcceptReplyCache) Get() *FastAcceptReply {
	var t *FastAcceptReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &FastAcceptReply{}
	}
	return t
}
func (p *FastAcceptReplyCache) Put(t *FastAcceptReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *FastAcceptReply) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:5]
	tmp32 := t.Ballot
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	bs[4] = byte(t.OK)
	wire.Write(bs)
	t.Id.Marshal(wire)
	bs = b[:]
	alen1 := int64(len(t.Cmd))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Cmd[i].Marshal(wire)
	}
	bs = b[:]
	alen2 := int64(len(t.Deps))
	if wlen := binary.PutVarint(bs, alen2); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen2; i++ {
		t.Deps[i].Marshal(wire)
	}
}

func (t *FastAcceptReply) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [10]byte
	var bs []byte
	bs = b[:5]
	if _, err := io.ReadAtLeast(wire, bs, 5); err != nil {
		return err
	}
	t.Ballot = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.OK = uint8(bs[4])
	t.Id.Unmarshal(wire)
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Cmd = make([]state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Cmd[i].Unmarshal(wire)
	}
	alen2, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Deps = make([]state.Id, alen2)
	for i := int64(0); i < alen2; i++ {
		t.Deps[i].Unmarshal(wire)
	}
	return nil
}

func (t *AcceptReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type AcceptReplyCache struct {
	mu    sync.Mutex
	cache []*AcceptReply
}

func NewAcceptReplyCache() *AcceptReplyCache {
	c := &AcceptReplyCache{}
	c.cache = make([]*AcceptReply, 0)
	return c
}

func (p *AcceptReplyCache) Get() *AcceptReply {
	var t *AcceptReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &AcceptReply{}
	}
	return t
}
func (p *AcceptReplyCache) Put(t *AcceptReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *AcceptReply) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:5]
	tmp32 := t.Ballot
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	bs[4] = byte(t.OK)
	wire.Write(bs)
	t.Id.Marshal(wire)
	bs = b[:]
	alen1 := int64(len(t.Cmd))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Cmd[i].Marshal(wire)
	}
	bs = b[:]
	alen2 := int64(len(t.Dep))
	if wlen := binary.PutVarint(bs, alen2); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen2; i++ {
		t.Dep[i].Marshal(wire)
	}
}

func (t *AcceptReply) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [10]byte
	var bs []byte
	bs = b[:5]
	if _, err := io.ReadAtLeast(wire, bs, 5); err != nil {
		return err
	}
	t.Ballot = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.OK = uint8(bs[4])
	t.Id.Unmarshal(wire)
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Cmd = make([]state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Cmd[i].Unmarshal(wire)
	}
	alen2, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Dep = make([]state.Id, alen2)
	for i := int64(0); i < alen2; i++ {
		t.Dep[i].Unmarshal(wire)
	}
	return nil
}

func (t *Commit) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type CommitCache struct {
	mu    sync.Mutex
	cache []*Commit
}

func NewCommitCache() *CommitCache {
	c := &CommitCache{}
	c.cache = make([]*Commit, 0)
	return c
}

func (p *CommitCache) Get() *Commit {
	var t *Commit
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Commit{}
	}
	return t
}
func (p *CommitCache) Put(t *Commit) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Commit) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:4]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Id.Marshal(wire)
	bs = b[:]
	alen1 := int64(len(t.Cmd))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Cmd[i].Marshal(wire)
	}
	bs = b[:]
	alen2 := int64(len(t.Deps))
	if wlen := binary.PutVarint(bs, alen2); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen2; i++ {
		t.Deps[i].Marshal(wire)
	}
}

func (t *Commit) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [10]byte
	var bs []byte
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Id.Unmarshal(wire)
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Cmd = make([]state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Cmd[i].Unmarshal(wire)
	}
	alen2, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Deps = make([]state.Id, alen2)
	for i := int64(0); i < alen2; i++ {
		t.Deps[i].Unmarshal(wire)
	}
	return nil
}

func (t *SyncReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 9, true
}

type SyncReplyCache struct {
	mu    sync.Mutex
	cache []*SyncReply
}

func NewSyncReplyCache() *SyncReplyCache {
	c := &SyncReplyCache{}
	c.cache = make([]*SyncReply, 0)
	return c
}

func (p *SyncReplyCache) Get() *SyncReply {
	var t *SyncReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &SyncReply{}
	}
	return t
}
func (p *SyncReplyCache) Put(t *SyncReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *SyncReply) Marshal(wire io.Writer) {
	var b [9]byte
	var bs []byte
	bs = b[:9]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	bs[8] = byte(t.OK)
	wire.Write(bs)
}

func (t *SyncReply) Unmarshal(wire io.Reader) error {
	var b [9]byte
	var bs []byte
	bs = b[:9]
	if _, err := io.ReadAtLeast(wire, bs, 9); err != nil {
		return err
	}
	t.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.OK = uint8(bs[8])
	return nil
}

func (t *FastAccept) New() fastrpc.Serializable {
	return new(FastAccept)
}

func (t *FastAcceptReply) New() fastrpc.Serializable {
	return new(FastAcceptReply)
}

func (t *Accept) New() fastrpc.Serializable {
	return new(Accept)
}

func (t *AcceptReply) New() fastrpc.Serializable {
	return new(AcceptReply)
}

func (t *Commit) New() fastrpc.Serializable {
	return new(Commit)
}

func (t *NewLeader) New() fastrpc.Serializable {
	return new(NewLeader)
}

func (t *NewLeaderReply) New() fastrpc.Serializable {
	return new(NewLeaderReply)
}

func (t *Sync) New() fastrpc.Serializable {
	return new(Sync)
}

func (t *SyncReply) New() fastrpc.Serializable {
	return new(SyncReply)
}
