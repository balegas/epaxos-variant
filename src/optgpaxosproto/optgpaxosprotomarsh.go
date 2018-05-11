package optgpaxosproto

import (
	"io"
	"sync"
	"bufio"
	"encoding/binary"
	"state"
	"fastrpc"
)

type byteReader interface {
	io.Reader
	ReadByte() (c byte, err error)
}

func (t *FastAccept) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type FastAcceptCache struct {
	mu	sync.Mutex
	cache	[]*FastAccept
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
	var b [12]byte
	var bs []byte
	bs = b[:12]
	tmp32 := t.Instance
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.LeaderId
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Id.Marshal(wire)
	bs = b[:]
	alen1 := int64(len(t.Command))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Command[i].Marshal(wire)
	}
}

func (t *FastAccept) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [12]byte
	var bs []byte
	bs = b[:12]
	if _, err := io.ReadAtLeast(wire, bs, 12); err != nil {
		return err
	}
	t.Instance = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.LeaderId = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Ballot = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.Id.Unmarshal(wire)
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Command = make([]state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Command[i].Unmarshal(wire)
	}
	return nil
}

func (t *FastAcceptReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type FastAcceptReplyCache struct {
	mu	sync.Mutex
	cache	[]*FastAcceptReply
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
	bs = b[:9]
	tmp32 := t.Instance
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	bs[4] = byte(t.OK)
	tmp32 = t.Ballot
	bs[5] = byte(tmp32)
	bs[6] = byte(tmp32 >> 8)
	bs[7] = byte(tmp32 >> 16)
	bs[8] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Id.Marshal(wire)
	bs = b[:]
	alen1 := int64(len(t.Deps))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
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
	bs = b[:9]
	if _, err := io.ReadAtLeast(wire, bs, 9); err != nil {
		return err
	}
	t.Instance = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.OK = uint8(bs[4])
	t.Ballot = int32((uint32(bs[5]) | (uint32(bs[6]) << 8) | (uint32(bs[7]) << 16) | (uint32(bs[8]) << 24)))
	t.Id.Unmarshal(wire)
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Deps = make([]state.Id, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Deps[i].Unmarshal(wire)
	}
	return nil
}

func (t *Accept) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type AcceptCache struct {
	mu	sync.Mutex
	cache	[]*Accept
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
	var b [12]byte
	var bs []byte
	bs = b[:12]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Instance
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Cmds.Marshal(wire)
}

func (t *Accept) Unmarshal(wire io.Reader) error {
	var b [12]byte
	var bs []byte
	bs = b[:12]
	if _, err := io.ReadAtLeast(wire, bs, 12); err != nil {
		return err
	}
	t.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Instance = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Ballot = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.Cmds.Unmarshal(wire)
	return nil
}

func (t *AcceptReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 9, true
}

type AcceptReplyCache struct {
	mu	sync.Mutex
	cache	[]*AcceptReply
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
	var b [9]byte
	var bs []byte
	bs = b[:9]
	tmp32 := t.Instance
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	bs[4] = byte(t.OK)
	tmp32 = t.Ballot
	bs[5] = byte(tmp32)
	bs[6] = byte(tmp32 >> 8)
	bs[7] = byte(tmp32 >> 16)
	bs[8] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *AcceptReply) Unmarshal(wire io.Reader) error {
	var b [9]byte
	var bs []byte
	bs = b[:9]
	if _, err := io.ReadAtLeast(wire, bs, 9); err != nil {
		return err
	}
	t.Instance = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.OK = uint8(bs[4])
	t.Ballot = int32((uint32(bs[5]) | (uint32(bs[6]) << 8) | (uint32(bs[7]) << 16) | (uint32(bs[8]) << 24)))
	return nil
}

func (t *Commit) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type CommitCache struct {
	mu	sync.Mutex
	cache	[]*Commit
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
	var b [12]byte
	var bs []byte
	bs = b[:12]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Instance
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Id.Marshal(wire)
	bs = b[:]
	alen1 := int64(len(t.Command))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Command[i].Marshal(wire)
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
	var b [12]byte
	var bs []byte
	bs = b[:12]
	if _, err := io.ReadAtLeast(wire, bs, 12); err != nil {
		return err
	}
	t.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Instance = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Ballot = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.Id.Unmarshal(wire)
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Command = make([]state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Command[i].Unmarshal(wire)
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

func (t *FullCommit) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type FullCommitCache struct {
	mu	sync.Mutex
	cache	[]*FullCommit
}

func NewFullCommitCache() *FullCommitCache {
	c := &FullCommitCache{}
	c.cache = make([]*FullCommit, 0)
	return c
}

func (p *FullCommitCache) Get() *FullCommit {
	var t *FullCommit
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &FullCommit{}
	}
	return t
}
func (p *FullCommitCache) Put(t *FullCommit) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *FullCommit) Marshal(wire io.Writer) {
	var b [12]byte
	var bs []byte
	bs = b[:12]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Instance
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Cmds.Marshal(wire)
}

func (t *FullCommit) Unmarshal(wire io.Reader) error {
	var b [12]byte
	var bs []byte
	bs = b[:12]
	if _, err := io.ReadAtLeast(wire, bs, 12); err != nil {
		return err
	}
	t.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Instance = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Ballot = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.Cmds.Unmarshal(wire)
	return nil
}

func (t *Prepare) BinarySize() (nbytes int, sizeKnown bool) {
	return 13, true
}

type PrepareCache struct {
	mu	sync.Mutex
	cache	[]*Prepare
}

func NewPrepareCache() *PrepareCache {
	c := &PrepareCache{}
	c.cache = make([]*Prepare, 0)
	return c
}

func (p *PrepareCache) Get() *Prepare {
	var t *Prepare
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Prepare{}
	}
	return t
}
func (p *PrepareCache) Put(t *Prepare) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Prepare) Marshal(wire io.Writer) {
	var b [13]byte
	var bs []byte
	bs = b[:13]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Instance
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	bs[12] = byte(t.ToInfinity)
	wire.Write(bs)
}

func (t *Prepare) Unmarshal(wire io.Reader) error {
	var b [13]byte
	var bs []byte
	bs = b[:13]
	if _, err := io.ReadAtLeast(wire, bs, 13); err != nil {
		return err
	}
	t.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Instance = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Ballot = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.ToInfinity = uint8(bs[12])
	return nil
}

func (t *PrepareReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type PrepareReplyCache struct {
	mu	sync.Mutex
	cache	[]*PrepareReply
}

func NewPrepareReplyCache() *PrepareReplyCache {
	c := &PrepareReplyCache{}
	c.cache = make([]*PrepareReply, 0)
	return c
}

func (p *PrepareReplyCache) Get() *PrepareReply {
	var t *PrepareReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &PrepareReply{}
	}
	return t
}
func (p *PrepareReplyCache) Put(t *PrepareReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *PrepareReply) Marshal(wire io.Writer) {
	var b [9]byte
	var bs []byte
	bs = b[:9]
	tmp32 := t.Instance
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	bs[4] = byte(t.OK)
	tmp32 = t.Ballot
	bs[5] = byte(tmp32)
	bs[6] = byte(tmp32 >> 8)
	bs[7] = byte(tmp32 >> 16)
	bs[8] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Cmds.Marshal(wire)
}

func (t *PrepareReply) Unmarshal(wire io.Reader) error {
	var b [9]byte
	var bs []byte
	bs = b[:9]
	if _, err := io.ReadAtLeast(wire, bs, 9); err != nil {
		return err
	}
	t.Instance = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.OK = uint8(bs[4])
	t.Ballot = int32((uint32(bs[5]) | (uint32(bs[6]) << 8) | (uint32(bs[7]) << 16) | (uint32(bs[8]) << 24)))
	t.Cmds.Unmarshal(wire)
	return nil
}

func (t *FullCommit) New() fastrpc.Serializable {
	return new(FullCommit)
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

func (t *Prepare) New() fastrpc.Serializable {
	return new(Prepare)
}

func (t *PrepareReply) New() fastrpc.Serializable {
	return new(PrepareReply)
}

func (t *AcceptReply) New() fastrpc.Serializable {
	return new(AcceptReply)
}

func (t *Commit) New() fastrpc.Serializable {
	return new(Commit)
}