package mingdb

import "os"

const FileName = "mingdb.data"
const MergeFileName = "mingdb.data.merge"

type DBFile struct {
	File   *os.File
	Offset int64
}

func newInternal(fileName string) (*DBFile, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	//因为要追加写入，所以offset刚开始是文件大小
	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}
	return &DBFile{File: file, Offset: stat.Size()}, nil
}

// 新建一个数据库文件
func NewDBFile(path string) (*DBFile, error) {
	fileName := path + string(os.PathSeparator) + FileName
	return newInternal(fileName)
}

// NewMergeDBFile 新建一个合并时的数据文件
func NewMergeDBFile(path string) (*DBFile, error) {
	fileName := path + string(os.PathSeparator) + MergeFileName
	return newInternal(fileName)
}

//
func (df *DBFile) Write(e *Entry) error {
	buf, err := e.Encode()
	if err != nil {
		return err
	}
	_, err = df.File.WriteAt(buf, df.Offset)
	if err != nil {
		return err
	}
	df.Offset += e.GetSize()
	return nil
}
func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	buf := make([]byte, entryHeaderSize)
	if _, err = df.File.ReadAt(buf, offset); err != nil {
		return
	}
	if e, err = Decode(buf); err != nil {
		return
	}
	offset += entryHeaderSize
	if e.KeySize > 0 {
		key := make([]byte, e.KeySize)
		if _, err = df.File.ReadAt(key, offset); err != nil {
			return
		}
		e.Key = key
	}
	offset += int64(e.KeySize)
	if e.ValueSize > 0 {
		value := make([]byte, e.ValueSize)
		if _, err = df.File.ReadAt(value, offset); err != nil {
			return
		}
		e.Value = value
	}
	return
}
