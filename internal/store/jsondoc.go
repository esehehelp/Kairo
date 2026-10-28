package store

import (
	"bytes"
	"encoding/json"
	"strings"
)

// JSON documents are stored as the text the code wrote. These helpers change
// one in Go, keeping the order of its members (what SQLite's JSON functions
// did in SQL), and write it compact.

type jsonMember struct {
	key   string
	value json.RawMessage
}

// jsonObject parses doc as an object into its members in order; ok is false
// when doc is not a JSON object.
func jsonObject(doc []byte) (members []jsonMember, ok bool) {
	if !json.Valid(doc) || !strings.HasPrefix(strings.TrimSpace(string(doc)), "{") {
		return nil, false
	}
	dec := json.NewDecoder(bytes.NewReader(doc))
	if _, err := dec.Token(); err != nil {
		return nil, false
	}
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return nil, false
		}
		var value json.RawMessage
		if err = dec.Decode(&value); err != nil {
			return nil, false
		}
		members = append(members, jsonMember{key: key.(string), value: value})
	}
	return members, true
}

// writeJSONObject writes members as a compact object.
func writeJSONObject(members []jsonMember) json.RawMessage {
	var out bytes.Buffer
	out.WriteByte('{')
	for i, m := range members {
		if i > 0 {
			out.WriteByte(',')
		}
		name, _ := json.Marshal(m.key)
		out.Write(name)
		out.WriteByte(':')
		if err := json.Compact(&out, m.value); err != nil {
			out.Write(m.value)
		}
	}
	out.WriteByte('}')
	return json.RawMessage(out.Bytes())
}

// jsonMergePatch applies patch to target (RFC 7396, as SQLite's json_patch):
// an object patch sets its members (recursively), a null member removes one;
// any other patch replaces target. New members come after the old ones.
func jsonMergePatch(target, patch []byte) json.RawMessage {
	patchMembers, ok := jsonObject(patch)
	if !ok {
		var compact bytes.Buffer
		if json.Compact(&compact, patch) != nil {
			return json.RawMessage(patch)
		}
		return json.RawMessage(compact.Bytes())
	}
	members, _ := jsonObject(target)
	for _, p := range patchMembers {
		i := 0
		for i < len(members) && members[i].key != p.key {
			i++
		}
		if string(bytes.TrimSpace(p.value)) == "null" {
			if i < len(members) {
				members = append(members[:i], members[i+1:]...)
			}
			continue
		}
		if i < len(members) {
			members[i].value = jsonMergePatch(members[i].value, p.value)
		} else {
			members = append(members, jsonMember{key: p.key, value: jsonMergePatch(nil, p.value)})
		}
	}
	return writeJSONObject(members)
}
