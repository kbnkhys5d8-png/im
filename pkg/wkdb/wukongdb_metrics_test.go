package wkdb

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/stretchr/testify/require"
)

func TestCollectMetricsUsesDatabaseMetrics(t *testing.T) {
	previousTrace := trace.GlobalTrace
	t.Cleanup(func() {
		trace.SetGlobalTrace(previousTrace)
	})

	database := NewWukongDB(NewOptions(
		WithDir(t.TempDir()),
		WithShardNum(1),
	)).(*wukongDB)
	require.NoError(t, database.Open())
	t.Cleanup(func() {
		require.NoError(t, database.Close())
	})

	trace.SetGlobalTrace(nil)
	require.NotPanics(t, database.collectMetrics)
}
