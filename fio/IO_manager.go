package fio

const DataFilePerm = 0644

// IOManager is the interface for file IO, can be implemented by different file IO strategy
type IOManager interface {
	// Read the file from the offset, and return the data
	Read([]byte, int64) (int, error)
	// Write the data to the file
	Write([]byte) (int, error)
	// Sync persist the data
	Sync() error
	// Close the file
	Close() error
	Size() (int64, error)
}

// InitIOManager init IO manager,support standard file system IO
func InitIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
