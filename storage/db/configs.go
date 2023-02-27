package db

const maxMapSize = 0xFFFFFFFFFFFF // 256TB
// maxAllocSize is the size used when creating array pointers.
const maxAllocSize = 0x7FFFFFFF

// The largest step that can be taken when remapping the mmap.
const maxMmapStep = 1 << 30 // 1GB
