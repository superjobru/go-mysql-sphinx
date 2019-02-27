package util

import (
	"strconv"

	"github.com/davecgh/go-spew/spew"
	"github.com/juju/errors"
)

// CoerceToUint32 does what it says
func CoerceToUint32(v interface{}) (uint32, error) {
	switch t := v.(type) {
	case int:
		return uint32(t), nil
	case int8:
		return uint32(t), nil
	case int16:
		return uint32(t), nil
	case int32:
		return uint32(t), nil
	case int64:
		return uint32(t), nil
	case uint:
		return uint32(t), nil
	case uint8:
		return uint32(t), nil
	case uint16:
		return uint32(t), nil
	case uint32:
		return uint32(t), nil
	case uint64:
		return uint32(t), nil
	case float32:
		return uint32(t), nil
	case float64:
		return uint32(t), nil
	case string:
		parsed, err := strconv.ParseUint(t, 10, 32)
		return uint32(parsed), err
	case []byte:
		parsed, err := strconv.ParseUint(string(t), 10, 32)
		return uint32(parsed), err
	case nil:
		return 0, nil
	default:
		return 0, errors.Errorf("could not cource %s to uint32", spew.Sdump(v))
	}
}

// CoerceToInt64 does what it says
func CoerceToInt64(v interface{}) (int64, error) {
	switch t := v.(type) {
	case int:
		return int64(t), nil
	case int8:
		return int64(t), nil
	case int16:
		return int64(t), nil
	case int32:
		return int64(t), nil
	case int64:
		return int64(t), nil
	case uint:
		return int64(t), nil
	case uint8:
		return int64(t), nil
	case uint16:
		return int64(t), nil
	case uint32:
		return int64(t), nil
	case uint64:
		return int64(t), nil
	case float32:
		return int64(t), nil
	case float64:
		return int64(t), nil
	case string:
		return strconv.ParseInt(t, 10, 64)
	case []byte:
		return strconv.ParseInt(string(t), 10, 64)
	case nil:
		return 0, nil
	default:
		return 0, errors.Errorf("could not cource %s to int64", spew.Sdump(v))
	}
}

// CoerceToUint64 does what it says
func CoerceToUint64(v interface{}) (uint64, error) {
	switch t := v.(type) {
	case int:
		return uint64(t), nil
	case int8:
		return uint64(t), nil
	case int16:
		return uint64(t), nil
	case int32:
		return uint64(t), nil
	case int64:
		return uint64(t), nil
	case uint:
		return uint64(t), nil
	case uint8:
		return uint64(t), nil
	case uint16:
		return uint64(t), nil
	case uint32:
		return uint64(t), nil
	case uint64:
		return uint64(t), nil
	case float32:
		return uint64(t), nil
	case float64:
		return uint64(t), nil
	case string:
		return strconv.ParseUint(t, 10, 64)
	case []byte:
		return strconv.ParseUint(string(t), 10, 64)
	case nil:
		return 0, nil
	default:
		return 0, errors.Errorf("could not cource %s to uint64", spew.Sdump(v))
	}
}
