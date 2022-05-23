package bugfruit

import "errors"

var (
	// ErrDBClosed is returned when a method that requires an open DB is called on a
	// closed DB.
	ErrDBClosed = errors.New("database is closed")

	// ErrInvalidMetaSlice is returned when a slice of bytes that doesn't fit the
	// metadata format is passed to a method that requires a slice of bytes that
	// require the metadata format.
	ErrInvalidMetaSlice = errors.New("invalid meta slice")

	// ErrInvalidKeyValSlice is returned when a slice of bytes that doesn't fit the
	// datum format is passed to a method that requires a slice of bytes that
	// require the datum format.
	ErrInvalidKeyValSlice = errors.New("invalid key/val slice")

	// ErrNoMetadata is returned when performing operations on a datum with no
	// metadata.
	ErrNoMetadata = errors.New("no metadata")
)
