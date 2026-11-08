package store

import (
	"encoding/base64"
	"encoding/json"
	"errors"
)

// A list cursor is where a page of a list ended: the sort key and id of its
// last row, opaque to clients (base64url of a JSON pair; a null key sorts as
// the list puts nulls). The next page is the rows after it.
type listCursor struct {
	key *string
	id  string
}

func (c listCursor) String() string {
	body, _ := json.Marshal([]any{c.key, c.id})
	return base64.RawURLEncoding.EncodeToString(body)
}

// ErrBadCursor is an after that is not a cursor a list page returned (a bad
// request).
var ErrBadCursor = errors.New("after is not a cursor a list returned")

func parseListCursor(raw string) (listCursor, error) {
	body, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return listCursor{}, ErrBadCursor
	}
	var pair []*string
	if json.Unmarshal(body, &pair) != nil || len(pair) != 2 || pair[1] == nil || *pair[1] == "" {
		return listCursor{}, ErrBadCursor
	}
	return listCursor{key: pair[0], id: *pair[1]}, nil
}
