// Package instant is a point in time as an execution declares it
// (not_before, start_by, finish_by): a TOML offset date-time or an RFC 3339
// string, kept as RFC 3339 with the offset it was written with, so a time
// shown back reads as it was declared.
package instant

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Instant is an RFC 3339 time with an explicit offset (seconds precision), or
// "" for none. A local date-time (no offset) is refused: the daemon and the
// CLI may be in different zones.
type Instant string

// UnmarshalTOML takes a TOML offset date-time or a string.
func (i *Instant) UnmarshalTOML(value any) error {
	switch v := value.(type) {
	case time.Time:
		if name := v.Location().String(); strings.HasSuffix(name, "-local") {
			return fmt.Errorf("time %s has no offset: write it with one (for example %s+09:00)", v.Format("2006-01-02T15:04:05"), v.Format("2006-01-02T15:04:05"))
		}
		*i = Instant(v.Truncate(time.Second).Format(time.RFC3339))
		return nil
	case string:
		normalized, err := Normalize(v)
		if err != nil {
			return err
		}
		*i = Instant(normalized)
		return nil
	default:
		return fmt.Errorf("a time must be an offset date-time, not %T", value)
	}
}

// Normalize checks an RFC 3339 time and returns it at seconds precision with
// its offset kept ("" stays "").
func Normalize(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return "", fmt.Errorf("time %q must be RFC 3339 with an offset (for example 2026-11-08T06:50:00+09:00)", value)
	}
	return t.Truncate(time.Second).Format(time.RFC3339), nil
}

// Time parses a normalized instant; ok is false for "" (or garbage).
func (i Instant) Time() (time.Time, bool) {
	return Parse(string(i))
}

// Parse parses an RFC 3339 time; ok is false for "" or garbage.
func Parse(value string) (time.Time, bool) {
	if value == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, value)
	return t, err == nil
}

// CheckOrder refuses a start_by or finish_by before not_before, and a
// finish_by before start_by. Every argument is normalized or "".
func CheckOrder(notBefore, startBy, finishBy string) error {
	nb, hasNB := Parse(notBefore)
	sb, hasSB := Parse(startBy)
	fb, hasFB := Parse(finishBy)
	switch {
	case hasNB && hasSB && sb.Before(nb):
		return errors.New("start_by is before not_before")
	case hasNB && hasFB && !fb.After(nb):
		return errors.New("finish_by is not after not_before")
	case hasSB && hasFB && fb.Before(sb):
		return errors.New("finish_by is before start_by")
	}
	return nil
}
