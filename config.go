package bugfruit

// Config encapsulates all config options for a Storage.
type Config struct {
	// VacuumBatch is the number of write operations between vaccuums. 0 turns off vacuuming.
	VacuumBatch uint64

	// FsyncBatch is the number of write operations between fsync calls. 0 turns off fsync, except on Close.
	FsyncBatch uint64
}
