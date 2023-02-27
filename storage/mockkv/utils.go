package mockkv

// NewKV external API for creating a local KV.
func NewKV(shardID string, len int) *Shard {
	return newShardKV(shardID, len)
}
