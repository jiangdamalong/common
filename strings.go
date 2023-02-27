package common

import "reflect"

//find a normal define string, end by 0
func GetStringFromArray(aval []byte) string {
	for i := 0; i < len(aval); i++ {
		if aval[i] == 0 {
			return string(aval[0:i])
		}
	}
	return string(aval)
}

//find a normal define string, end by 0
func GetStringFromBytesRv(aval reflect.Value) string {
	len := aval.Len()
	for i := 0; i < len; i++ {
		if aval.Index(i).Uint() == 0 {
			return string(aval.Slice(0, i).Bytes())
		}
	}
	return string(aval.Slice(0, len).Bytes())
}
