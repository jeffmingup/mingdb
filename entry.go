package mingdb

import "encoding/binary"

const entryHeaderSize = 10
const (
	PUT uint16 = iota
	DEL
)

type Entry struct {
	KeySize   uint32
	ValueSize uint32
	Mark      uint16
	Key       []byte
	Value     []byte
}

func NewEntry(key, value []byte, mark uint16) *Entry {
	return &Entry{
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
		Mark:      mark,
		Key:       key,
		Value:     value,
	}
}
func (e *Entry) GetSize() int64 {
	return int64(entryHeaderSize + e.KeySize + e.ValueSize)
}

func (e *Entry) Encode() ([]byte, error) {
	buf := make([]byte, e.GetSize())
	binary.BigEndian.PutUint32(buf[0:4], e.KeySize)
	binary.BigEndian.PutUint32(buf[4:8], e.ValueSize)
	binary.BigEndian.PutUint16(buf[8:10], e.Mark)
	copy(buf[entryHeaderSize:entryHeaderSize+e.KeySize], e.Key)
	copy(buf[entryHeaderSize+e.KeySize:], e.Value)
	return buf, nil
}
func Decode(buf []byte) (*Entry, error) {
	keySize := binary.BigEndian.Uint32(buf[0:4])
	valueSize := binary.BigEndian.Uint32(buf[4:8])
	mark := binary.BigEndian.Uint16(buf[8:10])
	return &Entry{KeySize: keySize, ValueSize: valueSize, Mark: mark}, nil
}
