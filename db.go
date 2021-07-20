package mingdb

import (
	"io"
	"log"
	"os"
	"sync"
)

type DB struct {
	dbFile  *DBFile
	indexes map[string]int64 //内存中的索引信息
	dirPath string           //数据库文件目录
	mu      sync.RWMutex
}

func Open(dirPath string) (*DB, error) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	dbFile, err := NewDBFile(dirPath)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	db := &DB{
		dbFile:  dbFile,
		indexes: make(map[string]int64),
		dirPath: dirPath,
	}
	db.LoadIndexesFromFile(db.dbFile)
	return db, nil
}
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	offset := db.dbFile.Offset
	entry := NewEntry(key, value, PUT)

	//追加到数据文件中
	if err := db.dbFile.Write(entry); err != nil {
		return err
	}

	//写到内存中   文件偏移量和key对应，方便查询
	db.indexes[string(key)] = offset
	return nil
}
func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()

	offset, ok := db.indexes[string(key)]
	if !ok {
		return nil, nil
	}
	e, err := db.dbFile.Read(offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return e.Value, nil

}
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	_, ok := db.indexes[string(key)]
	if !ok {
		return nil
	}
	entry := NewEntry(key, nil, DEL)
	//追加到数据文件中
	if err := db.dbFile.Write(entry); err != nil {
		return err
	}
	delete(db.indexes, string(key))
	return nil
}
func (db *DB) LoadIndexesFromFile(dbFile *DBFile) error {
	if dbFile == nil {
		return nil
	}
	var offset int64

	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		db.indexes[string(e.Key)] = offset
		if e.Mark == DEL {
			delete(db.indexes, string(e.Key))
		}
		offset += e.GetSize()
	}
	return nil
}
func (db *DB) Merge() error {
	if db.dbFile.Offset == 0 {
		return nil
	}
	var validEntry []*Entry
	var offset int64
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if off, ok := db.indexes[string(e.Key)]; ok && off == offset {
			validEntry = append(validEntry, e)
		}
		offset += e.GetSize()
	}
	if len(validEntry) > 0 {
		mergeDBFile, err := NewMergeDBFile(db.dirPath)
		if err != nil {
			return err
		}
		defer os.Remove(mergeDBFile.File.Name())
		for _, entry := range validEntry {
			writeOff := mergeDBFile.Offset
			if err := mergeDBFile.Write(entry); err != nil {
				return err
			}
			db.indexes[string(entry.Key)] = writeOff
		}
		// 删除旧的数据文件
		os.Remove(db.dbFile.File.Name())
		// 临时文件变更为新的数据文件
		os.Rename(mergeDBFile.File.Name(), db.dirPath+string(os.PathSeparator)+FileName)

		db.dbFile = mergeDBFile

	}
	return nil

}
