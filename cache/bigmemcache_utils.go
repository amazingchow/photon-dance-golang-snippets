package bigmemcache

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

func findNearestPowerOf2Num(n uint) uint {
	if (n & (n - 1)) == 0 {
		return n
	}
	k := uint(1)
	for (k << 1) < n {
		k <<= 1
	}
	return k
}

func featureID2HashKey(id int64) string {
	return strconv.FormatInt(id, 16)
}

func (c *BigMemCache) encode(fe *Feature) ([]byte, error) {
	/*
		type Feature struct {
			Version     int32
			ID          int64
			Meta        []byte
			Blob        []byte
			CreatedTime int64
		}
	*/
	totalLen := 4 + // totalLen
		4 + // Version
		8 + // ID
		2 + len(fe.Meta) +
		2 + len(fe.Blob) +
		8 // CreatedTime
	raw := make([]byte, totalLen)

	/*

	 */

	pos := 0
	binary.LittleEndian.PutUint32(raw[pos:], uint32(totalLen))
	pos += 4

	binary.LittleEndian.PutUint32(raw[pos:], uint32(fe.Version))
	pos += 4

	binary.LittleEndian.PutUint64(raw[pos:], uint64(fe.ID))
	pos += 8

	// 对于[]byte或string类型来说, 先存大小, 再存实际的字节
	binary.LittleEndian.PutUint16(raw[pos:], uint16(len(fe.Meta)))
	pos += 2
	copy(raw[pos:], []byte(fe.Meta))
	pos += len(fe.Meta)

	binary.LittleEndian.PutUint16(raw[pos:], uint16(len(fe.Blob)))
	pos += 2
	copy(raw[pos:], []byte(fe.Blob))
	pos += len(fe.Blob)

	binary.LittleEndian.PutUint64(raw[pos:], uint64(fe.CreatedTime))
	pos += 8

	if pos != totalLen {
		return nil, fmt.Errorf("failed to encode feature, Pos(%v) != TotalLen(%v)", pos, totalLen)
	}

	return raw, nil
}

func (c *BigMemCache) decode(raw []byte) (*Feature, error) {
	/*
		type Feature struct {
			Version     int32
			ID          int64
			Meta        []byte
			Blob        []byte
			CreatedTime int64
		}
	*/
	totalLen := len(raw)

	pos := 0
	storedTotalLen := binary.LittleEndian.Uint32(raw[pos:])
	if storedTotalLen != uint32(totalLen) {
		return nil, fmt.Errorf("StoredTotalLen(%v) != TotalLen(%v)", storedTotalLen, totalLen)
	}
	pos += 4

	var fe Feature

	fe.Version = int32(binary.LittleEndian.Uint32(raw[pos:]))
	pos += 4

	fe.ID = int64(binary.LittleEndian.Uint64(raw[pos:]))
	pos += 8

	metaLen := binary.LittleEndian.Uint16(raw[pos:])
	pos += 2
	fe.Meta = raw[pos : pos+int(metaLen)]
	pos += int(metaLen)

	blobLen := binary.LittleEndian.Uint16(raw[pos:])
	pos += 2
	fe.Meta = raw[pos : pos+int(blobLen)]
	pos += int(blobLen)

	fe.CreatedTime = int64(binary.LittleEndian.Uint64(raw[pos:]))
	pos += 8

	if pos != totalLen {
		return nil, fmt.Errorf("failed to decode feature, Pos(%v) != StoredTotalLen(%v)", pos, totalLen)
	}

	return &fe, nil
}
