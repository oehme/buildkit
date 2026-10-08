package resolver

import (
	"context"
	"io"
	"mime"
	"net/http"
	"path"
	"strings"

	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/remotes/docker"
	remoteserrors "github.com/containerd/containerd/v2/core/remotes/errors"
	"github.com/containerd/containerd/v2/pkg/reference"
	"github.com/moby/buildkit/util/tracing"
	"github.com/moby/buildkit/version"
	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

// maxManifestSize bounds the manifests fetched by FetchManifest. It matches
// the limit that cache importers apply to manifest blobs.
const maxManifestSize = 1 << 20

var manifestAccept = strings.Join([]string{
	images.MediaTypeDockerSchema2Manifest,
	images.MediaTypeDockerSchema2ManifestList,
	ocispecs.MediaTypeImageManifest,
	ocispecs.MediaTypeImageIndex, "*/*",
}, ", ")

// FetchManifest resolves a reference and fetches its manifest with a single
// request, where Resolve followed by a fetch takes two. The returned
// descriptor carries the manifest in its Data field, so that reading it
// through a content provider does not request it again.
func (r *Resolver) FetchManifest(ctx context.Context, ref string) (_ string, _ ocispecs.Descriptor, err error) {
	span, ctx := tracing.StartSpan(ctx, "fetching manifest "+ref)
	defer func() { tracing.FinishWithError(span, err) }()

	refspec, err := reference.Parse(ref)
	if err != nil {
		return "", ocispecs.Descriptor{}, err
	}
	if refspec.Object == "" {
		return "", ocispecs.Descriptor{}, errors.Errorf("reference %s has neither a tag nor a digest", ref)
	}
	hosts, err := r.HostsFunc(refspec.Hostname())
	if err != nil {
		return "", ocispecs.Descriptor{}, err
	}
	ctx, err = docker.ContextWithRepositoryScope(ctx, refspec, false)
	if err != nil {
		return "", ocispecs.Descriptor{}, err
	}

	var firstErr error
	for _, host := range hosts {
		if !host.Capabilities.Has(docker.HostCapabilityResolve) {
			continue
		}
		desc, err := fetchManifest(ctx, host, refspec, r.headers)
		if err == nil {
			r.handler.counter.Add(1)
			return refspec.String(), desc, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if firstErr == nil {
		firstErr = errors.Errorf("no registry host can resolve %s", ref)
	}
	return "", ocispecs.Descriptor{}, firstErr
}

func fetchManifest(ctx context.Context, host docker.RegistryHost, refspec reference.Spec, headers http.Header) (ocispecs.Descriptor, error) {
	object := refspec.Object
	if dgst := refspec.Digest(); dgst != "" {
		object = dgst.String()
	}
	repository := strings.TrimPrefix(refspec.Locator, refspec.Hostname()+"/")
	u := host.Scheme + "://" + path.Join(host.Host, host.Path, repository, "manifests", object)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return ocispecs.Descriptor{}, err
	}
	for k, v := range headers {
		req.Header[k] = append(req.Header[k], v...)
	}
	for k, v := range host.Header {
		req.Header[k] = append(req.Header[k], v...)
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", manifestAccept)
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", version.UserAgent())
	}

	resp, err := doAuthorized(ctx, host, req)
	if err != nil {
		return ocispecs.Descriptor{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ocispecs.Descriptor{}, remoteserrors.NewUnexpectedStatusErr(resp)
	}

	dt, err := io.ReadAll(io.LimitReader(resp.Body, maxManifestSize+1))
	if err != nil {
		return ocispecs.Descriptor{}, err
	}
	if len(dt) > maxManifestSize {
		return ocispecs.Descriptor{}, errors.Errorf("manifest %s is larger than %d bytes", refspec, maxManifestSize)
	}

	dgst := digest.FromBytes(dt)
	if hdr := resp.Header.Get("Docker-Content-Digest"); hdr != "" {
		claimed, err := digest.Parse(hdr)
		if err != nil {
			return ocispecs.Descriptor{}, errors.Wrapf(err, "invalid Docker-Content-Digest header for %s", refspec)
		}
		if claimed.Algorithm().Available() {
			dgst = claimed.Algorithm().FromBytes(dt)
		}
		if dgst != claimed {
			return ocispecs.Descriptor{}, errors.Errorf("manifest %s has digest %s but the registry claims %s", refspec, dgst, claimed)
		}
	}
	if want := refspec.Digest(); want != "" && want != dgst {
		return ocispecs.Descriptor{}, errors.Errorf("manifest %s has digest %s", refspec, dgst)
	}

	mediaType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	return ocispecs.Descriptor{
		MediaType: mediaType,
		Digest:    dgst,
		Size:      int64(len(dt)),
		Data:      dt,
	}, nil
}

// doAuthorized sends the request with the credentials the host's authorizer
// knows, and once more after answering an authentication challenge.
func doAuthorized(ctx context.Context, host docker.RegistryHost, req *http.Request) (*http.Response, error) {
	if host.Client == nil {
		return nil, errors.Errorf("registry host %s has no client", host.Host)
	}
	send := func() (*http.Response, error) {
		attempt := req.Clone(ctx)
		if host.Authorizer != nil {
			if err := host.Authorizer.Authorize(ctx, attempt); err != nil {
				return nil, err
			}
		}
		return host.Client.Do(attempt)
	}
	resp, err := send()
	if err != nil || resp.StatusCode != http.StatusUnauthorized || host.Authorizer == nil {
		return resp, err
	}
	err = host.Authorizer.AddResponses(ctx, []*http.Response{resp})
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	return send()
}
