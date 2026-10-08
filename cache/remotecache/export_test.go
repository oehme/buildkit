package remotecache

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

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
