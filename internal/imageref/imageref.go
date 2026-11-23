// Package imageref reads OCI image references and pins them to a digest.
//
// A task that runs in a container names its image by a reference such as
// "python:3.12-slim" or "ghcr.io/org/app:1.2"; the image a tag names changes
// when it is pushed again, so `kairo project apply` resolves the tag to the
// digest of what it names now (Resolve) and the task is bound to that
// digest for good, as it is to its code snapshot.
package imageref

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// DockerHub is the registry a reference without one names.
const DockerHub = "docker.io"

var (
	component = regexp.MustCompile(`^[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*$`)
	tagRE     = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)
	digestRE  = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	domainRE  = regexp.MustCompile(`^(?:localhost|[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)*)(?::[0-9]{1,5})?$`)
)

// Ref is an image reference: a repository of a registry, and a tag or a
// digest (or both, the digest deciding).
type Ref struct {
	Registry   string // "docker.io", "ghcr.io", "localhost:5000"
	Repository string // "library/python", "org/app"
	Tag        string
	Digest     string // "sha256:<64 hex>"
}

// Parse reads a reference as docker does: a first path element with a dot,
// a colon or "localhost" is the registry (else Docker Hub, where a
// one-element repository is under "library/"); a tag after the last colon
// of the last element ("latest" when neither tag nor digest is given); a
// digest after "@".
func Parse(s string) (Ref, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Ref{}, errors.New("empty image reference")
	}
	var r Ref
	name := s
	if at := strings.LastIndex(s, "@"); at >= 0 {
		name, r.Digest = s[:at], s[at+1:]
		if !digestRE.MatchString(r.Digest) {
			return Ref{}, fmt.Errorf("image %q: digest %q is not sha256:<64 lowercase hex>", s, r.Digest)
		}
	}
	if slash := strings.LastIndex(name, "/"); strings.LastIndex(name, ":") > slash {
		colon := strings.LastIndex(name, ":")
		name, r.Tag = name[:colon], name[colon+1:]
		if !tagRE.MatchString(r.Tag) {
			return Ref{}, fmt.Errorf("image %q: tag %q is not a valid tag", s, r.Tag)
		}
	}
	parts := strings.Split(name, "/")
	if len(parts) > 1 && (strings.ContainsAny(parts[0], ".:") || parts[0] == "localhost") {
		r.Registry, parts = parts[0], parts[1:]
		if !domainRE.MatchString(r.Registry) {
			return Ref{}, fmt.Errorf("image %q: registry %q is not a host name", s, r.Registry)
		}
	} else {
		r.Registry = DockerHub
	}
	if r.Registry == "index.docker.io" || r.Registry == "registry-1.docker.io" {
		r.Registry = DockerHub
	}
	if r.Registry == DockerHub && len(parts) == 1 {
		parts = []string{"library", parts[0]}
	}
	for _, p := range parts {
		if !component.MatchString(p) {
			return Ref{}, fmt.Errorf("image %q: repository element %q is not lowercase letters, digits and separators", s, p)
		}
	}
	r.Repository = strings.Join(parts, "/")
	if r.Tag == "" && r.Digest == "" {
		r.Tag = "latest"
	}
	return r, nil
}

// ParsePinned reads a reference that must name a digest.
func ParsePinned(s string) (Ref, error) {
	r, err := Parse(s)
	if err != nil {
		return Ref{}, err
	}
	if r.Digest == "" {
		return Ref{}, fmt.Errorf("image %q is not pinned to a digest (registry/repository@sha256:...)", s)
	}
	return r, nil
}

// String is the reference written out in full: registry/repository, then
// :tag and @digest when set.
func (r Ref) String() string {
	s := r.Registry + "/" + r.Repository
	if r.Tag != "" {
		s += ":" + r.Tag
	}
	if r.Digest != "" {
		s += "@" + r.Digest
	}
	return s
}

// Pinned is the reference by its digest alone (registry/repository@digest),
// "" when it has none.
func (r Ref) Pinned() string {
	if r.Digest == "" {
		return ""
	}
	return r.Registry + "/" + r.Repository + "@" + r.Digest
}

// SameRepository reports whether a and b name the same repository.
func SameRepository(a, b Ref) bool { return a.Registry == b.Registry && a.Repository == b.Repository }

// apiHost is where a registry serves the registry API.
func apiHost(registry string) string {
	if registry == DockerHub {
		return "registry-1.docker.io"
	}
	return registry
}

// manifestTypes are the manifests asked for: an index (several platforms)
// is pinned as the index, so every platform keeps its image.
var manifestTypes = []string{
	"application/vnd.oci.image.index.v1+json",
	"application/vnd.docker.distribution.manifest.list.v2+json",
	"application/vnd.oci.image.manifest.v1+json",
	"application/vnd.docker.distribution.manifest.v2+json",
}

// Resolve pins r to the digest of what it names now (r itself when it has
// one), asking its registry anonymously (a bearer token for pulling public
// images, when the registry asks for one). Client nil: a client with a
// one-minute timeout.
func Resolve(ctx context.Context, client *http.Client, r Ref) (Ref, error) {
	if r.Digest != "" {
		return r, nil
	}
	if client == nil {
		client = &http.Client{Timeout: time.Minute}
	}
	scheme := "https"
	if strings.HasPrefix(r.Registry, "localhost") || strings.HasPrefix(r.Registry, "127.0.0.1") {
		scheme = "http"
	}
	u := scheme + "://" + apiHost(r.Registry) + "/v2/" + r.Repository + "/manifests/" + r.Tag
	digest, err := manifestDigest(ctx, client, u, "")
	if challenge, ok := errors.AsType[*bearerChallenge](err); ok {
		token, terr := fetchToken(ctx, client, challenge, r.Repository)
		if terr != nil {
			return Ref{}, fmt.Errorf("image %s: %w", r, terr)
		}
		digest, err = manifestDigest(ctx, client, u, token)
	}
	if err != nil {
		return Ref{}, fmt.Errorf("image %s: %w", r, err)
	}
	r.Digest = digest
	return r, nil
}

// bearerChallenge is a 401 asking for a bearer token.
type bearerChallenge struct{ realm, service, scope string }

func (c *bearerChallenge) Error() string { return "the registry asks for a token from " + c.realm }

// manifestDigest asks for the manifest at u and returns its digest: the
// Docker-Content-Digest header, else the sha256 of the body.
func manifestDigest(ctx context.Context, client *http.Client, u, token string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", strings.Join(manifestTypes, ", "))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode == http.StatusUnauthorized && token == "" {
		if c := parseChallenge(resp.Header.Get("WWW-Authenticate")); c != nil {
			return "", c
		}
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%s: %s", u, resp.Status)
	}
	if d := strings.TrimSpace(resp.Header.Get("Docker-Content-Digest")); d != "" {
		if !digestRE.MatchString(d) {
			return "", fmt.Errorf("%s: the registry answered digest %q", u, d)
		}
		sum := sha256.Sum256(body)
		if "sha256:"+hex.EncodeToString(sum[:]) != d {
			return "", fmt.Errorf("%s: the manifest does not hash to its digest %s", u, d)
		}
		return d, nil
	}
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// parseChallenge reads `Bearer realm="...",service="...",scope="..."`.
func parseChallenge(h string) *bearerChallenge {
	scheme, rest, ok := strings.Cut(strings.TrimSpace(h), " ")
	if !ok || !strings.EqualFold(scheme, "bearer") {
		return nil
	}
	c := &bearerChallenge{}
	for _, part := range splitParams(rest) {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		value = strings.Trim(strings.TrimSpace(value), `"`)
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "realm":
			c.realm = value
		case "service":
			c.service = value
		case "scope":
			c.scope = value
		}
	}
	if c.realm == "" {
		return nil
	}
	return c
}

// splitParams splits challenge parameters on the commas outside quotes.
func splitParams(s string) []string {
	var out []string
	var b strings.Builder
	quoted := false
	for _, r := range s {
		switch {
		case r == '"':
			quoted = !quoted
			b.WriteRune(r)
		case r == ',' && !quoted:
			out = append(out, b.String())
			b.Reset()
		default:
			b.WriteRune(r)
		}
	}
	return append(out, b.String())
}

// fetchToken asks the challenge's realm for an anonymous pull token.
func fetchToken(ctx context.Context, client *http.Client, c *bearerChallenge, repository string) (string, error) {
	u, err := url.Parse(c.realm)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return "", fmt.Errorf("token realm %q is not a URL", c.realm)
	}
	q := u.Query()
	if c.service != "" {
		q.Set("service", c.service)
	}
	scope := c.scope
	if scope == "" {
		scope = "repository:" + repository + ":pull"
	}
	q.Set("scope", scope)
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token from %s: %s", u.Host, resp.Status)
	}
	var body struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body); err != nil {
		return "", fmt.Errorf("token from %s: %w", u.Host, err)
	}
	if body.Token != "" {
		return body.Token, nil
	}
	if body.AccessToken != "" {
		return body.AccessToken, nil
	}
	return "", fmt.Errorf("token from %s: no token in the answer", u.Host)
}
