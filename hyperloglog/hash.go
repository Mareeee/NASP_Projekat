package hll

import (
	"crypto/md5"
	"encoding/binary"
)

func Hash(data []byte) uint64 {
	hash := md5.Sum(data)

	// Convert the first 8 bytes of the hash to a uint64 in big-endian order
	hashUint := binary.BigEndian.Uint64(hash[:8])

	return hashUint
}
