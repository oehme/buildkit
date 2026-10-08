package solver

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/session"
	digest "github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// gatedCacheManager stands in for an imported cache whose manifest is still
// being downloaded: every lookup blocks until release is called.
type gatedCacheManager struct {
	CacheManager
	ready       chan struct{}
	releaseOnce sync.Once
	queries     atomic.Int64
	records     atomic.Int64
}

func newGatedCacheManager(cm CacheManager) *gatedCacheManager {
	return &gatedCacheManager{CacheManager: cm, ready: make(chan struct{})}
}

func (g *gatedCacheManager) release() {
	g.releaseOnce.Do(func() { close(g.ready) })
}

func (g *gatedCacheManager) consulted() bool {
	return g.queries.Load() > 0 || g.records.Load() > 0
}

func (g *gatedCacheManager) Query(inp []CacheKeyWithSelector, inputIndex Index, dgst digest.Digest, outputIndex Index) ([]*CacheKey, error) {
	<-g.ready
	g.queries.Add(1)
	return g.CacheManager.Query(inp, inputIndex, dgst, outputIndex)
}

func (g *gatedCacheManager) Records(ctx context.Context, ck *CacheKey) ([]*CacheRecord, error) {
	<-g.ready
	g.records.Add(1)
	return g.CacheManager.Records(ctx, ck)
}

// faultyResultStore lets individual results appear pruned, or fail to load
// like a ref that disappeared between the record lookup and the load.
type faultyResultStore struct {
	CacheResultStorage
	pruned     sync.Map
	unloadable sync.Map
}

func (s *faultyResultStore) Exists(ctx context.Context, id string) bool {
	if _, ok := s.pruned.Load(id); ok {
		return false
	}
	return s.CacheResultStorage.Exists(ctx, id)
}

func (s *faultyResultStore) Load(ctx context.Context, res CacheResult) (Result, error) {
	if _, ok := s.unloadable.Load(res.ID); ok {
		return nil, errors.New("result is gone")
	}
	return s.CacheResultStorage.Load(ctx, res)
}

// dockerfileLikeGraph mirrors FROM, RUN, COPY . and RUN. The context vertex
// gets a random cache key that differs per build, like a local source, while
// its content stays the same so the content-based key of COPY matches.
func dockerfileLikeGraph(build, valueSuffix string, cacheSource CacheManager) Edge {
	base := vtx(vtxOpt{name: "base", cacheKeySeed: "base", value: "base" + valueSuffix, cacheSource: cacheSource})
	run1 := vtx(vtxOpt{name: "run1", cacheKeySeed: "run1", value: "run1" + valueSuffix, cacheSource: cacheSource, inputs: []Edge{{Vertex: base}}})
	buildContext := vtx(vtxOpt{name: "context-" + build, cacheKeySeed: "context-" + build, randomCacheKey: true, value: "sources", cacheSource: cacheSource})
	cp := vtx(vtxOpt{
		name:             "copy",
		cacheKeySeed:     "copy",
		value:            "copy" + valueSuffix,
		cacheSource:      cacheSource,
		inputs:           []Edge{{Vertex: run1}, {Vertex: buildContext}},
		slowCacheCompute: map[int]ResultBasedCacheFunc{1: digestFromResult},
	})
	run2 := vtx(vtxOpt{name: "run2", cacheKeySeed: "run2", value: "run2" + valueSuffix, cacheSource: cacheSource, inputs: []Edge{{Vertex: cp}}})
	return Edge{Vertex: run2}
}

// twoInputStep builds a vertex with two inputs, like COPY --from, whose cache
// key seeds can be varied independently.
func twoInputStep(name, seedA, seedB, valueSuffix string, cacheSource CacheManager, opts ...func(*vtxOpt, *vtxOpt, *vtxOpt)) Edge {
	a := vtxOpt{name: name + "-a-" + seedA, cacheKeySeed: seedA, value: "a-" + seedA, cacheSource: cacheSource}
	b := vtxOpt{name: name + "-b-" + seedB, cacheKeySeed: seedB, value: "b-" + seedB, cacheSource: cacheSource}
	step := vtxOpt{name: name, cacheKeySeed: "step", value: "step" + valueSuffix, cacheSource: cacheSource}
	for _, opt := range opts {
		opt(&a, &b, &step)
	}
	step.inputs = []Edge{{Vertex: vtx(a)}, {Vertex: vtx(b)}}
	return Edge{Vertex: vtx(step)}
}

func withContentKeys(counter *atomic.Int64) func(*vtxOpt, *vtxOpt, *vtxOpt) {
	countingDigest := func(ctx context.Context, res Result, g session.Group) (digest.Digest, error) {
		counter.Add(1)
		return digestFromResult(ctx, res, g)
	}
	return func(_, _, step *vtxOpt) {
		step.slowCacheCompute = map[int]ResultBasedCacheFunc{0: countingDigest, 1: countingDigest}
	}
}

func withIgnoreCacheB(_, b, _ *vtxOpt) {
	b.ignoreCache = true
}

func withNoCache(a, b, step *vtxOpt) {
	a.ignoreCache = true
	b.ignoreCache = true
	step.ignoreCache = true
}

func newSolverWithCache(t *testing.T, cm CacheManager) *Solver {
	t.Helper()
	s := NewSolver(SolverOpt{ResolveOpFunc: testOpResolver, DefaultCache: cm})
	t.Cleanup(s.Close)
	return s
}

func populatedCache(t *testing.T, graphs ...Edge) CacheManager {
	t.Helper()
	cm := NewInMemoryCacheManager()
	s := newSolverWithCache(t, cm)
	for _, g := range graphs {
		buildOnce(t, s, g)
	}
	return cm
}

func buildOnce(t *testing.T, s *Solver, g Edge) Result {
	t.Helper()
	j, err := s.NewJob(identity.NewID())
	require.NoError(t, err)
	defer j.Discard()
	res, err := j.Build(t.Context(), g)
	require.NoError(t, err)
	return res
}

func buildWithTimeout(t *testing.T, s *Solver, g Edge, onTimeout func()) Result {
	t.Helper()
	j, err := s.NewJob(identity.NewID())
	require.NoError(t, err)
	defer j.Discard()

	var res Result
	done := make(chan error, 1)
	go func() {
		var err error
		res, err = j.Build(t.Context(), g)
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		onTimeout()
		t.Fatal("build blocked on the imported cache")
	}
	return res
}

func execCount(g Edge) int64 {
	return atomic.LoadInt64(g.Vertex.(*vertex).execCallCount)
}

func TestImportedCacheNotConsultedWhenMainCacheMatches(t *testing.T) {
	t.Parallel()

	s := newSolverWithCache(t, NewInMemoryCacheManager())
	warmup := dockerfileLikeGraph("1", "", nil)
	warmup.Vertex.(*vertex).setupCallCounters()
	require.Equal(t, "run2", unwrap(buildOnce(t, s, warmup)))
	require.Equal(t, int64(5), execCount(warmup))

	imported := newGatedCacheManager(NewInMemoryCacheManager())
	defer imported.release()

	g := dockerfileLikeGraph("2", "-fresh", imported)
	g.Vertex.(*vertex).setupCallCounters()
	res := buildWithTimeout(t, s, g, imported.release)

	require.Equal(t, "run2", unwrap(res))
	require.Equal(t, int64(1), execCount(g), "only the context should run")
	require.False(t, imported.consulted(), "imported cache must not be consulted for a locally cached build")
}

func TestImportedCacheConsultedOnMainCacheMiss(t *testing.T) {
	t.Parallel()

	imported := newGatedCacheManager(populatedCache(t, dockerfileLikeGraph("1", "-imported", nil)))
	imported.release()

	s := newSolverWithCache(t, NewInMemoryCacheManager())
	g := dockerfileLikeGraph("2", "-fresh", imported)
	g.Vertex.(*vertex).setupCallCounters()
	res := buildOnce(t, s, g)

	require.Equal(t, "run2-imported", unwrap(res))
	require.Equal(t, int64(1), execCount(g), "only the context should run")
	require.True(t, imported.consulted())

	// loading from the imported cache populates the main cache
	g3 := dockerfileLikeGraph("3", "-fresh", nil)
	g3.Vertex.(*vertex).setupCallCounters()
	require.Equal(t, "run2-imported", unwrap(buildOnce(t, s, g3)))
	require.Equal(t, int64(1), execCount(g3))
}

// The main cache answers every input of the step, but with results that were
// produced from other input combinations. Only the imported cache holds a
// result for this combination, and it has to be found from the definition
// based keys, before any content based key is computed.
func TestImportedCacheConsultedWhenMainMatchesDoNotIntersect(t *testing.T) {
	t.Parallel()

	var contentKeys atomic.Int64
	main := populatedCache(t,
		twoInputStep("main1", "a1", "b1", "-main", nil, withContentKeys(&contentKeys)),
		twoInputStep("main2", "a2", "b2", "-main", nil, withContentKeys(&contentKeys)),
	)
	imported := newGatedCacheManager(populatedCache(t, twoInputStep("remote", "a1", "b2", "-imported", nil, withContentKeys(&contentKeys))))
	imported.release()
	contentKeys.Store(0)

	g := twoInputStep("fresh", "a1", "b2", "-fresh", imported, withContentKeys(&contentKeys))
	g.Vertex.(*vertex).setupCallCounters()
	res := buildOnce(t, newSolverWithCache(t, main), g)

	require.Equal(t, "step-imported", unwrap(res))
	require.Equal(t, int64(0), execCount(g))
	require.Equal(t, int64(0), contentKeys.Load(), "imported cache must be consulted before content based keys are computed")
	require.True(t, imported.consulted())
}

// The main cache answers the first input of the step but not the second. The
// probe for the first input has to be repeated against the imported cache so
// that the step can match there.
func TestImportedCacheConsultedWhenMainMatchesOnlySomeInputs(t *testing.T) {
	t.Parallel()

	main := populatedCache(t, twoInputStep("main", "a1", "b1", "-main", nil))
	imported := newGatedCacheManager(populatedCache(t, twoInputStep("remote", "a1", "b2", "-imported", nil)))
	imported.release()

	g := twoInputStep("fresh", "a1", "b2", "-fresh", imported)
	g.Vertex.(*vertex).setupCallCounters()
	res := buildOnce(t, newSolverWithCache(t, main), g)

	require.Equal(t, "step-imported", unwrap(res))
	require.Equal(t, int64(0), execCount(g))
	require.True(t, imported.consulted())
}

// One input of the step ignores the cache, so the step can never match and
// must not wait for the imported cache.
func TestImportedCacheNotConsultedWhenNoMatchIsPossible(t *testing.T) {
	t.Parallel()

	main := populatedCache(t, twoInputStep("main", "a1", "b1", "-main", nil))
	imported := newGatedCacheManager(NewInMemoryCacheManager())
	defer imported.release()

	g := twoInputStep("fresh", "a1", "b1", "-fresh", imported, withIgnoreCacheB)
	g.Vertex.(*vertex).setupCallCounters()
	res := buildWithTimeout(t, newSolverWithCache(t, main), g, imported.release)

	require.Equal(t, "step-fresh", unwrap(res))
	require.Equal(t, int64(2), execCount(g), "the ignore-cache input and the step should run")
	require.False(t, imported.consulted())
}

// A build that ignores the cache, like --no-cache, has nothing to gain from the
// imported cache and must not wait for it, even though its content based keys
// still get recorded.
func TestImportedCacheNotConsultedWhenCacheIsIgnored(t *testing.T) {
	t.Parallel()

	var contentKeys atomic.Int64
	main := populatedCache(t, twoInputStep("main", "a1", "b1", "-main", nil, withContentKeys(&contentKeys)))
	imported := newGatedCacheManager(NewInMemoryCacheManager())
	defer imported.release()

	g := twoInputStep("fresh", "a1", "b1", "-fresh", imported, withContentKeys(&contentKeys), withNoCache)
	g.Vertex.(*vertex).setupCallCounters()
	res := buildWithTimeout(t, newSolverWithCache(t, main), g, imported.release)

	require.Equal(t, "step-fresh", unwrap(res))
	require.Equal(t, int64(3), execCount(g))
	require.False(t, imported.consulted())
}

// Steps whose results were never saved locally, like the stages of a
// multi-stage build that the final stage does not depend on at load time, are
// known to the main cache by key only. As long as they do not have to execute,
// the imported cache must not be consulted for them.
func TestImportedCacheNotConsultedForKeysWithoutResultsOfUnneededSteps(t *testing.T) {
	t.Parallel()

	mainResults := &faultyResultStore{CacheResultStorage: NewInMemoryResultStorage()}
	main := NewCacheManager(t.Context(), "main", NewInMemoryCacheStorage(), mainResults)
	s := newSolverWithCache(t, main)

	root := vtx(vtxOpt{name: "root", cacheKeySeed: "root", value: "root"})
	middle := vtx(vtxOpt{name: "middle", cacheKeySeed: "middle", value: "middle", inputs: []Edge{{Vertex: root}}})
	final := vtx(vtxOpt{name: "final", cacheKeySeed: "final", value: "final", inputs: []Edge{{Vertex: middle}}})
	buildOnce(t, s, Edge{Vertex: final})
	mainResults.pruned.Store(buildOnce(t, s, Edge{Vertex: root}).ID(), struct{}{})
	mainResults.pruned.Store(buildOnce(t, s, Edge{Vertex: middle}).ID(), struct{}{})

	imported := newGatedCacheManager(NewInMemoryCacheManager())
	defer imported.release()

	root2 := vtx(vtxOpt{name: "root", cacheKeySeed: "root", value: "root-fresh", cacheSource: imported})
	middle2 := vtx(vtxOpt{name: "middle", cacheKeySeed: "middle", value: "middle-fresh", cacheSource: imported, inputs: []Edge{{Vertex: root2}}})
	g := Edge{Vertex: vtx(vtxOpt{name: "final", cacheKeySeed: "final", value: "final-fresh", cacheSource: imported, inputs: []Edge{{Vertex: middle2}}})}
	g.Vertex.(*vertex).setupCallCounters()
	res := buildWithTimeout(t, s, g, imported.release)

	require.Equal(t, "final", unwrap(res))
	require.Equal(t, int64(0), execCount(g))
	require.False(t, imported.consulted())
}

// The main cache still knows the key of the step, but its result has been
// pruned. The records of the imported cache have to be consulted instead.
func TestImportedCacheRecordsConsultedWhenMainResultIsGone(t *testing.T) {
	t.Parallel()

	mainResults := &faultyResultStore{CacheResultStorage: NewInMemoryResultStorage()}
	main := NewCacheManager(t.Context(), "main", NewInMemoryCacheStorage(), mainResults)
	s := newSolverWithCache(t, main)
	pruned := buildOnce(t, s, twoInputStep("main", "a1", "b1", "-main", nil))
	mainResults.pruned.Store(pruned.ID(), struct{}{})

	imported := newGatedCacheManager(populatedCache(t, twoInputStep("remote", "a1", "b1", "-imported", nil)))
	imported.release()

	g := twoInputStep("fresh", "a1", "b1", "-fresh", imported)
	g.Vertex.(*vertex).setupCallCounters()
	res := buildOnce(t, s, g)

	require.Equal(t, "step-imported", unwrap(res))
	require.Equal(t, int64(0), execCount(g))
	require.True(t, imported.consulted())
}

// The main cache has a record for the step, but loading it fails. The step
// falls back to the record of the imported cache instead of executing.
func TestImportedCacheConsultedWhenMainResultFailsToLoad(t *testing.T) {
	t.Parallel()

	mainResults := &faultyResultStore{CacheResultStorage: NewInMemoryResultStorage()}
	main := NewCacheManager(t.Context(), "main", NewInMemoryCacheStorage(), mainResults)
	s := newSolverWithCache(t, main)
	broken := buildOnce(t, s, twoInputStep("main", "a1", "b1", "-main", nil))
	mainResults.unloadable.Store(broken.ID(), struct{}{})

	imported := newGatedCacheManager(populatedCache(t, twoInputStep("remote", "a1", "b1", "-imported", nil)))
	imported.release()

	g := twoInputStep("fresh", "a1", "b1", "-fresh", imported)
	g.Vertex.(*vertex).setupCallCounters()
	res := buildOnce(t, s, g)

	require.Equal(t, "step-imported", unwrap(res))
	require.Equal(t, int64(0), execCount(g))
	require.True(t, imported.consulted())
}

func TestCombinedCacheManagerQueryMain(t *testing.T) {
	t.Parallel()

	main := NewInMemoryCacheManager()
	imported := NewInMemoryCacheManager()

	_, final, err := NewCombinedCacheManager([]CacheManager{main}, main).(mainCacheQuerier).QueryMain(nil, 0, digest.FromString("x"), 0)
	require.NoError(t, err)
	require.True(t, final, "without imported caches the main cache answer is final")

	_, final, err = NewCombinedCacheManager([]CacheManager{main, imported}, main).(mainCacheQuerier).QueryMain(nil, 0, digest.FromString("x"), 0)
	require.NoError(t, err)
	require.False(t, final)

	_, final, err = NewCombinedCacheManager([]CacheManager{imported}, nil).(mainCacheQuerier).QueryMain(nil, 0, digest.FromString("x"), 0)
	require.NoError(t, err)
	require.False(t, final)
}

func TestCanMatchImportedCache(t *testing.T) {
	t.Parallel()

	random := NewCacheKey(digest.Digest(randomDigestPrefix+"abc"), "", 0)
	stable := NewCacheKey(digest.FromString("stable"), "", 0)
	inputs := func(keys ...*CacheKey) []CacheKeyWithSelector {
		out := make([]CacheKeyWithSelector, len(keys))
		for i, k := range keys {
			out[i] = CacheKeyWithSelector{CacheKey: ExportableCacheKey{CacheKey: k}}
		}
		return out
	}

	require.True(t, canMatchImportedCache(nil, digest.FromString("stable")))
	require.False(t, canMatchImportedCache(nil, digest.Digest(randomDigestPrefix+"abc")))
	require.True(t, canMatchImportedCache(inputs(stable), digest.FromString("step")))
	require.False(t, canMatchImportedCache(inputs(random), digest.FromString("step")))
	require.True(t, canMatchImportedCache(inputs(random, stable), digest.FromString("step")))
}
