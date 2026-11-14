package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"strings"
)

func normalizeObject(raw json.RawMessage, allowEmpty bool) (json.RawMessage, error) {
	if len(raw) == 0 && allowEmpty {
		raw = json.RawMessage(`{}`)
	}
	value, err := decodeJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("JSON object: %w: %v", ErrInvalid, err)
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, fmt.Errorf("expected JSON object: %w", ErrInvalid)
	}
	return json.Marshal(value)
}

func decodeJSON(raw json.RawMessage) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, errors.New("unexpected trailing JSON data")
	}
	return value, nil
}

func sameJSON(left, right json.RawMessage) bool {
	a, err := decodeJSON(left)
	if err != nil {
		return false
	}
	b, err := decodeJSON(right)
	if err != nil {
		return false
	}
	return reflect.DeepEqual(normalizeNumbers(a), normalizeNumbers(b))
}

func normalizeNumbers(value any) any {
	switch value := value.(type) {
	case json.Number:
		return json.Number(canonicalNumber(value))
	case map[string]any:
		for key, item := range value {
			value[key] = normalizeNumbers(item)
		}
	case []any:
		for index, item := range value {
			value[index] = normalizeNumbers(item)
		}
	}
	return value
}

// canonicalNumber normalizes decimals without rounding or expanding exponents.
func canonicalNumber(number json.Number) string {
	power := big.Int{}
	str := strings.ToLower(string(number))
	mantissa, exponent, found := strings.Cut(str, "e")
	if found {
		power.SetString(exponent, 10)
	}

	negative := strings.HasPrefix(mantissa, "-")
	mantissa = strings.TrimPrefix(mantissa, "-")
	if point := strings.IndexByte(mantissa, '.'); point >= 0 {
		power.Sub(&power, big.NewInt(int64(len(mantissa)-point-1)))
		mantissa = mantissa[:point] + mantissa[point+1:]
	}

	mantissa = strings.TrimLeft(mantissa, "0")
	if mantissa == "" {
		return "0"
	}

	trimmed := strings.TrimRight(mantissa, "0")
	power.Add(&power, big.NewInt(int64(len(mantissa)-len(trimmed))))
	if negative {
		trimmed = "-" + trimmed
	}
	return trimmed + "e" + power.String()
}
