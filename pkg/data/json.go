package data

import "encoding/json"

func MarshalData(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}
