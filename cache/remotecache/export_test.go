package remotecache

import (
	"bytes"
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	cacheimporttypes "github.com/moby/buildkit/cache/remotecache/v1/types"
	"github.com/moby/buildkit/util/contentutil"
	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

func TestConfigDescriptorEmbedsSmallConfig(t *testing.T) {
	dt := []byte(`{"layers":[],"records":[]}`)

	desc := configDescriptor(dt, true)
	require.Equal(t, digest.FromBytes(dt), desc.Digest)
	require.Equal(t, int64(len(dt)), desc.Size)
	require.Equal(t, cacheimporttypes.CacheConfigMediaTypeV0, desc.MediaType)
	require.Equal(t, dt, desc.Data)

	require.Nil(t, configDescriptor(dt, false).Data, "docker media types do not define embedded data")
	require.Nil(t, configDescriptor(bytes.Repeat([]byte("x"), maxEmbeddedConfigSize+1), true).Data)
}

// The importer reads an embedded config from the manifest and never requests
// the config blob, which the registry would answer with a redirect to blob
// storage.
func TestImportReadsEmbeddedConfig(t *testing.T) {
	ctx := context.Background()
	config := []byte(`{"layers":[],"records":[]}`)

	manifestWithConfig := func(desc ocispecs.Descriptor) (content.Provider, ocispecs.Descriptor) {
		dt, err := json.Marshal(ocispecs.Manifest{
			MediaType: ocispecs.MediaTypeImageManifest,
			Config:    desc,
		})
		require.NoError(t, err)
		mfstDesc := ocispecs.Descriptor{MediaType: ocispecs.MediaTypeImageManifest, Digest: digest.FromBytes(dt), Size: int64(len(dt))}
		buf := contentutil.NewBuffer()
		require.NoError(t, content.WriteBlob(ctx, buf, "manifest", bytes.NewReader(dt), mfstDesc))
		return buf, mfstDesc
	}

	provider, mfstDesc := manifestWithConfig(configDescriptor(config, true))
	cm, err := NewImporter(provider).Resolve(ctx, mfstDesc, "embedded", nil)
	require.NoError(t, err)
	require.NotNil(t, cm)

	provider, mfstDesc = manifestWithConfig(configDescriptor(config, false))
	_, err = NewImporter(provider).Resolve(ctx, mfstDesc, "referenced", nil)
	require.Error(t, err, "without embedded data the config blob has to be fetched")
}

// gatedLabelSetter blocks every label write until released.
type gatedLabelSetter struct {
	content.Provider
	release chan struct{}
	labeled atomic.Int64
}

func (g *gatedLabelSetter) SetDistributionSourceLabel(ctx context.Context, _ digest.Digest) error {
	select {
	case <-g.release:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	g.labeled.Add(1)
	return nil
}

func (g *gatedLabelSetter) SetDistributionSourceAnnotation(desc ocispecs.Descriptor) ocispecs.Descriptor {
	return desc
}

// The import must not wait for the distribution source labels of the layers
// to be written, but they must be written eventually.
func TestImportDoesNotWaitForDistributionSourceLabels(t *testing.T) {
	ctx := context.Background()
	config := []byte(`{"layers":[],"records":[]}`)
	layers := []ocispecs.Descriptor{
		{MediaType: ocispecs.MediaTypeImageLayerGzip, Digest: digest.FromString("layer-1"), Size: 1},
		{MediaType: ocispecs.MediaTypeImageLayerGzip, Digest: digest.FromString("layer-2"), Size: 1},
	}
	dt, err := json.Marshal(ocispecs.Manifest{
		MediaType: ocispecs.MediaTypeImageManifest,
		Config:    configDescriptor(config, true),
		Layers:    layers,
	})
	require.NoError(t, err)
	mfstDesc := ocispecs.Descriptor{MediaType: ocispecs.MediaTypeImageManifest, Digest: digest.FromBytes(dt), Size: int64(len(dt))}
	buf := contentutil.NewBuffer()
	require.NoError(t, content.WriteBlob(ctx, buf, "manifest", bytes.NewReader(dt), mfstDesc))

	provider := &gatedLabelSetter{Provider: buf, release: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err := NewImporter(provider).Resolve(ctx, mfstDesc, "gated", nil)
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		close(provider.release)
		t.Fatal("import waited for the distribution source labels")
	}
	require.Equal(t, int64(0), provider.labeled.Load())

	close(provider.release)
	require.Eventually(t, func() bool { return provider.labeled.Load() == int64(len(layers)) }, 10*time.Second, 10*time.Millisecond)
}
