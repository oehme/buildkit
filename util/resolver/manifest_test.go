package resolver

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/remotes/docker"
	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type noProvider struct{}

func (noProvider) ReaderAt(context.Context, ocispecs.Descriptor) (content.ReaderAt, error) {
	return nil, errors.New("the manifest must not be requested again")
}

// A registry that challenges the first request and serves the manifest, with
// its digest header, to the authenticated retry.
func newManifestRegistry(t *testing.T, manifest []byte, digestHeader string) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var manifestRequests atomic.Int64
	var tokenURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			_, _ = io.WriteString(w, `{"token":"token-1","expires_in":300}`)
		case "/v2/test/manifests/latest":
			manifestRequests.Add(1)
			if r.Method != http.MethodGet {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			if r.Header.Get("Authorization") != "Bearer token-1" {
				w.Header().Set("WWW-Authenticate", `Bearer realm="`+tokenURL+`",service="test",scope="repository:test:pull"`)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", ocispecs.MediaTypeImageManifest)
			if digestHeader != "" {
				w.Header().Set("Docker-Content-Digest", digestHeader)
			}
			_, _ = w.Write(manifest)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	tokenURL = srv.URL + "/token"
	return srv, &manifestRequests
}

func resolverFor(t *testing.T, srv *httptest.Server) (*Resolver, string) {
	t.Helper()
	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	hosts := func(string) ([]docker.RegistryHost, error) {
		return []docker.RegistryHost{{
			Client:       srv.Client(),
			Host:         u.Host,
			Scheme:       u.Scheme,
			Path:         "/v2",
			Capabilities: docker.HostCapabilityPull | docker.HostCapabilityResolve,
		}}, nil
	}
	ref := u.Host + "/test:latest"
	return NewPool().GetResolver(hosts, ref, ScopeType{}, nil, nil), ref
}

func TestFetchManifest(t *testing.T) {
	manifest := []byte(`{"schemaVersion":2,"mediaType":"` + ocispecs.MediaTypeImageManifest + `","config":{},"layers":[]}`)
	srv, manifestRequests := newManifestRegistry(t, manifest, digest.FromBytes(manifest).String())
	r, ref := resolverFor(t, srv)

	xref, desc, err := r.FetchManifest(t.Context(), ref)
	require.NoError(t, err)
	require.Equal(t, ref, xref)
	require.Equal(t, digest.FromBytes(manifest), desc.Digest)
	require.Equal(t, int64(len(manifest)), desc.Size)
	require.Equal(t, ocispecs.MediaTypeImageManifest, desc.MediaType)
	require.Equal(t, int64(2), manifestRequests.Load(), "one challenged request and one authenticated request")

	dt, err := content.ReadBlob(t.Context(), noProvider{}, desc)
	require.NoError(t, err)
	require.Equal(t, manifest, dt)
}

func TestFetchManifestRejectsDigestMismatch(t *testing.T) {
	manifest := []byte(`{"schemaVersion":2}`)
	srv, _ := newManifestRegistry(t, manifest, digest.FromString("something else").String())
	r, ref := resolverFor(t, srv)

	_, _, err := r.FetchManifest(t.Context(), ref)
	require.ErrorContains(t, err, "claims")
}

func TestFetchManifestReportsMissingManifest(t *testing.T) {
	srv, _ := newManifestRegistry(t, nil, "")
	r, ref := resolverFor(t, srv)

	_, _, err := r.FetchManifest(t.Context(), ref+"-missing")
	require.Error(t, err)
}
