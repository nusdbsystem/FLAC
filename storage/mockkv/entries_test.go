package mockkv

import (
	"FLAC/configs"
	"testing"
	"time"
)

func newEntriesTestKit() *shardEntry {
	From := newShardKV("id", 10)
	return newShardEntry(1, -1, -1, From)
}

func TestKVEntries(t *testing.T) {
	c := newEntriesTestKit()
	go func() {
		for i := 0; i < 100; i++ {
			c.update(NoTXNMark, 2)
		}
	}()
	go func() {
		for i := 0; i < 100; i++ {
			c.update(NoTXNMark, 3)
		}
	}()
	for i := 0; i < 200; i++ {
		val, ok := c.read(true)
		if ok {
			configs.Assert(val == 2 || val == 3 || val == -1, "The read value is invalid")
		}
		time.Sleep(time.Millisecond)
	}
}
