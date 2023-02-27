package utils

import (
	"FLAC/configs"
	"hash/fnv"
)

func hash(s string) int {
	h := fnv.New32a()
	_, err := h.Write([]byte(s))
	configs.CheckError(err)
	return int(h.Sum32()) % configs.NumberOfRecordsPerShard
}

func TransTableItem(tableName string, ware int, key int) uint64 {
	if tableName == "order" {
		return uint64(key)
	} else {
		return uint64(100000 + 10000*ware + key)
	}
}
