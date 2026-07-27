package plugin

import "testing"

func TestRunSearchSourceOfflineBootstrap60000ScansWithin121Pages(t *testing.T) {
	fixture := newSearchSourceBootstrapScanFixture(searchSourceBootstrapBenchmarkChannelCount)
	fixture.resetMetrics()

	if err := fixture.run(); err != nil {
		t.Fatal(err)
	}
	if fixture.pageReads > 121 {
		t.Fatalf("page reads = %d, want at most 121", fixture.pageReads)
	}
	if fixture.maxBatch > 500 {
		t.Fatalf("maximum returned batch = %d, want at most 500", fixture.maxBatch)
	}
	if fixture.appliedReads != searchSourceBootstrapBenchmarkChannelCount ||
		fixture.physicalReads != searchSourceBootstrapBenchmarkChannelCount {
		t.Fatalf(
			"watermark reads = applied:%d physical:%d, want %d each",
			fixture.appliedReads,
			fixture.physicalReads,
			searchSourceBootstrapBenchmarkChannelCount,
		)
	}
	if fixture.finalAppliedReads != 1 || fixture.finalPhysicalReads != 1 {
		t.Fatalf(
			"final channel watermark reads = applied:%d physical:%d, want 1 each",
			fixture.finalAppliedReads,
			fixture.finalPhysicalReads,
		)
	}
	if fixture.authorityCalls != 2*searchSourceBootstrapBenchmarkChannelCount ||
		fixture.finalAuthorityCalls != 2 {
		t.Fatalf(
			"authority calls = total:%d final:%d, want %d/2",
			fixture.authorityCalls,
			fixture.finalAuthorityCalls,
			2*searchSourceBootstrapBenchmarkChannelCount,
		)
	}
	wantSnapshotChecks := searchSourceBootstrapBenchmarkChannelCount + fixture.pageReads + 2
	if fixture.rosterCalls != wantSnapshotChecks || fixture.revisionReads != wantSnapshotChecks {
		t.Fatalf(
			"stable snapshot checks = roster:%d revision:%d, want %d each",
			fixture.rosterCalls,
			fixture.revisionReads,
			wantSnapshotChecks,
		)
	}
}
