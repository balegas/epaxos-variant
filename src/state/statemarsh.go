package state

import (
	"encoding/binary"
	"io"
	"bytes"
	"encoding/gob"
)

func (t *Command) Marshal(w io.Writer) {
	t.Op.Marshal(w)
	t.K.Marshal(w)
	t.V.Marshal(w)
}

func (t *Command) Unmarshal(r io.Reader) error {

    err := t.Op.Unmarshal(r)
    if err!=nil{
        return err
    }

    err = t.K.Unmarshal(r)
    if err!=nil{
        return err
    }

    err = t.V.Unmarshal(r)
    if err!=nil{
        return err
    }

    return nil
}

func (f *FullCmds) Marshal(w io.Writer) {
    var buf bytes.Buffer
    enc := gob.NewEncoder(&buf)
    enc.Encode(f.C)
    enc.Encode(f.D)
    w.Write(buf.Bytes())
}

func (f *FullCmds) Unmarshal(r io.Reader) error {
    dec := gob.NewDecoder(r)
    dec.Decode(&f.C)
    dec.Decode(&f.D)
    return nil
}

func (i *Id) Marshal(w io.Writer) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(i)
	w.Write(buf.Bytes())
}

func (i *Id) Unmarshal(r io.Reader) error {
	dec := gob.NewDecoder(r)
	dec.Decode(&i)
	return nil
}

func (t *Operation) Marshal(w io.Writer) {
	bs := make([]byte,1)
	bs[0] = byte(*t)
	w.Write(bs)
}

func (t *Operation) Unmarshal(r io.Reader) error {
	bs := make([]byte,1)
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	*t = Operation(bs[0])
	return nil
}

func (t *Key) Marshal(w io.Writer) {
	bs := make([]byte,8)
	binary.LittleEndian.PutUint64(bs, uint64(*t))
	w.Write(bs)
}

func (t *Key) Unmarshal(r io.Reader) error {
	bs := make([]byte,8)
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	*t = Key(binary.LittleEndian.Uint64(bs))
	return nil
}

func (t *Value) Marshal(w io.Writer) {
	bs := make([]byte,4)
	if t==nil{
		binary.LittleEndian.PutUint16(bs, 0)
		w.Write(bs)
	}else {
		binary.LittleEndian.PutUint16(bs, uint16(len(*t)))
		w.Write(bs)
		w.Write(*t)
	}
}

func (t *Value) Unmarshal(r io.Reader) error {
	bs := make([]byte,4)
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	len := binary.LittleEndian.Uint16(bs)
	bs = make([]byte,len)
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	*t = Value(bs)
	return nil
}