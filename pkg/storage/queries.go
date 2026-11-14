package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// pageCursor binds a continuation key to a query.
type pageCursor struct {
	Version int    `json:"v"`
	Scope   string `json:"scope"`
	Key     []byte `json:"key"`
}

// readRecord returns a value from one read transaction.
func readRecord[T any](
	ctx context.Context, store *Store, get func(*Tx) (T, error),
) (T, error) {
	var record T
	err := store.view(ctx, func(tx *Tx) error {
		var err error
		record, err = get(tx)
		return err
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return record, nil
}

// readPage reads a filtered page in one transaction.
func readPage[T Record](
	ctx context.Context, store *Store, prefix, scope []byte, page PageRequest,
	match func(T) bool, checkParent func(*Tx) error,
) (Page[T], error) {
	size, last, err := parsePage(page, prefix, scope)
	if err != nil {
		return Page[T]{}, err
	}

	var record T
	table := record.BucketName()
	result := Page[T]{Items: make([]T, 0, size)}
	err = store.view(ctx, func(tx *Tx) error {
		if checkParent != nil {
			if err := checkParent(tx); err != nil {
				return err
			}
		}

		cursor := tx.raw.Bucket([]byte(table)).Cursor()
		key, value := cursor.Seek(prefix)
		if last != nil {
			key, value = cursor.Seek(last)
			if bytes.Equal(key, last) {
				key, value = cursor.Next()
			}
		}

		var lastReturned []byte
		for ; key != nil && bytes.HasPrefix(key, prefix); key, value = cursor.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}

			var record T
			if err := json.Unmarshal(value, &record); err != nil {
				return fmt.Errorf("decode %s: %w: %v", table, ErrCorrupt, err)
			}
			if match != nil && !match(record) {
				continue
			}

			if len(result.Items) == size {
				result.NextPageToken = makePageToken(scope, lastReturned)
				return nil
			}
			result.Items = append(result.Items, record)
			lastReturned = bytes.Clone(key)
		}
		return nil
	})
	if err != nil {
		return Page[T]{}, err
	}
	return result, nil
}

// parsePage returns the page size and continuation key.
func parsePage(page PageRequest, prefix, scope []byte) (int, []byte, error) {
	page, err := page.Normalize()
	if err != nil {
		return 0, nil, err
	}
	if page.Token == "" {
		return page.Size, nil, nil
	}

	data, err := base64.RawURLEncoding.Strict().DecodeString(page.Token)
	if err != nil {
		return 0, nil, fmt.Errorf("page token: %w", ErrInvalid)
	}

	cursor := pageCursor{}
	if err := json.Unmarshal(data, &cursor); err != nil {
		return 0, nil, fmt.Errorf("page token: %w: %w", ErrInvalid, err)
	}

	if cursor.Version != 1 ||
		cursor.Scope != base64.RawURLEncoding.EncodeToString(scope) ||
		len(cursor.Key) == 0 ||
		!bytes.HasPrefix(cursor.Key, prefix) {
		return 0, nil, fmt.Errorf("page token: %w", ErrInvalid)
	}
	return page.Size, cursor.Key, nil
}

// makePageToken encodes a scoped continuation key.
func makePageToken(scope, key []byte) string {
	data, _ := json.Marshal(pageCursor{
		Version: 1,
		Scope:   base64.RawURLEncoding.EncodeToString(scope),
		Key:     key,
	})
	return base64.RawURLEncoding.EncodeToString(data)
}
