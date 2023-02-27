package db

import (
	"fmt"
	"os"
	"sync"
	"time"
)

type Options struct {
	ReadOnly        bool
	TimeOut         time.Duration
	InitialMmapSize int
	NoGrowSync      bool
	MmapFlags       int
}

var DefaultOptions = Options{ReadOnly: false, InitialMmapSize: 0x1000, NoGrowSync: false}

type DB struct {
	options    *Options
	NoGrowSync bool
	MmapFlags  int
	mmapLock   *sync.Mutex

	path   string
	file   *os.File
	opened bool

	pageSize int
	pagePool sync.Pool
}

func (db *DB) Close() error {
	return nil
}

func (db *DB) Init() error {
	return nil
}

// Open creates and open a database at the given path.
func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	var db = &DB{opened: true, options: options, path: path}

	if options == nil {
		db.options = &DefaultOptions
	}

	db.mmapLock = &sync.Mutex{}
	db.NoGrowSync = options.NoGrowSync
	db.MmapFlags = options.MmapFlags
	flag := os.O_RDWR
	if db.options.ReadOnly {
		flag = os.O_RDONLY
	}

	var err error
	if db.file, err = os.OpenFile(db.path, flag|os.O_CREATE, mode); err != nil {
		_ = db.Close()
		return nil, err
	}

	if err = fLock(db.file, !db.options.ReadOnly, db.options.TimeOut); err != nil {
		_ = db.Close()
		return nil, err
	}

	var info os.FileInfo
	if info, err = db.file.Stat(); err != nil {
		return nil, err
	} else if info.Size() == 0 {
		if err = db.Init(); err != nil {
			return nil, err
		}
	} else {
		// Read the meta page.
		db.pageSize = os.Getpagesize()
		var buf [0x1000]byte
		if _, err = db.file.ReadAt(buf[:], 0); err != nil {
			return nil, err
		}
	}

	db.pagePool = sync.Pool{New: func() interface{} {
		return make([]byte, db.pageSize)
	}}

	if err = db.mmap(options.InitialMmapSize); err != nil {
		_ = db.Close()
		return nil, err
	}
	// TODO: add free list into the database.

	return db, nil
}

func (db *DB) mmapSize(size int) (int, error) {
	// Double the size from 32KB until 1GB.
	for i := uint(15); i <= 30; i++ {
		if size <= 1<<i {
			return 1 << i, nil
		}
	}

	// Verify the requested size is not above the maximum allowed.
	if size > maxMapSize {
		return 0, fmt.Errorf("mmap too large")
	}

	// If larger than 1GB then grow by 1GB at a time.
	sz := int64(size)
	if remainder := sz % int64(maxMmapStep); remainder > 0 {
		sz += int64(maxMmapStep) - remainder
	}

	// Ensure that the mmap size is a multiple of the page size.
	// This should always be true since we're incrementing in MBs.
	pageSize := int64(db.pageSize)
	if (sz % pageSize) != 0 {
		sz = ((sz / pageSize) + 1) * pageSize
	}

	// If we've exceeded the max size then only grow up to the max size.
	if sz > maxMapSize {
		sz = maxMapSize
	}

	return int(sz), nil
}

// mmap opens the underlying memory-mapped file and initializes the meta references.
// minsz is the minimum size that the new mmap can be.
func (db *DB) mmap(minsz int) error {
	db.mmapLock.Lock()
	defer db.mmapLock.Unlock()

	inf, err := db.file.Stat()
	if err != nil {
		return fmt.Errorf("mmap stat error: %s", err)
	} else if int(inf.Size()) < db.pageSize*2 {
		return fmt.Errorf("file size too small")
	}

	// Ensure the size is at least the minimum size.
	var size = int(inf.Size())
	if size < minsz {
		size = minsz
	}
	size, err = db.mmapSize(size)
	if err != nil {
		return err
	}
	return nil
}
