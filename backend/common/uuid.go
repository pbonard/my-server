package common

import "github.com/google/uuid"

// UUIDsOf the ids as the services and grpc keep them, 16 bytes each
func UUIDsOf(ids [][]byte) []uuid.UUID {
	if ids == nil {
		return nil
	}
	us := make([]uuid.UUID, len(ids))
	for i, id := range ids {
		copy(us[i][:], id)
	}
	return us
}

// BytesOf the ids, 16 bytes each, as the services and grpc keep them
func BytesOf(ids []uuid.UUID) [][]byte {
	if ids == nil {
		return nil
	}
	bs := make([][]byte, len(ids))
	for i := range ids {
		bs[i] = ids[i][:]
	}
	return bs
}
