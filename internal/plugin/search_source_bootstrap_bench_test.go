package plugin

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

const searchSourceBootstrapBenchmarkChannelCount = 60_000

type searchSourceBootstrapScanFixture struct {
	configs       []wkdb.ChannelClusterConfig
	configIndexes map[string]int

	pageReads           int
	maxBatch            int
	appliedReads        int
	physicalReads       int
	authorityCalls      int
	rosterCalls         int
	revisionReads       int
	finalAppliedReads   int
	finalPhysicalReads  int
	finalAuthorityCalls int
}

func newSearchSourceBootstrapScanFixture(channelCount int) *searchSourceBootstrapScanFixture {
	configs := make([]wkdb.ChannelClusterConfig, channelCount)
	configIndexes := make(map[string]int, channelCount)
	replicas := []uint64{1}
	for index := range configs {
		channelID := fmt.Sprintf("channel-%05d", index+1)
		configs[index] = wkdb.ChannelClusterConfig{
			Id:              uint64(index + 1),
			ChannelId:       channelID,
			ChannelType:     2,
			ReplicaMaxCount: 1,
			Replicas:        replicas,
			LeaderId:        1,
			Term:            2,
			ConfVersion:     3,
			Status:          wkdb.ChannelClusterStatusNormal,
		}
		configIndexes[channelID] = index
	}
	return &searchSourceBootstrapScanFixture{
		configs:       configs,
		configIndexes: configIndexes,
	}
}

func (f *searchSourceBootstrapScanFixture) resetMetrics() {
	f.pageReads = 0
	f.maxBatch = 0
	f.appliedReads = 0
	f.physicalReads = 0
	f.authorityCalls = 0
	f.rosterCalls = 0
	f.revisionReads = 0
	f.finalAppliedReads = 0
	f.finalPhysicalReads = 0
	f.finalAuthorityCalls = 0
}

func (f *searchSourceBootstrapScanFixture) run() error {
	return runSearchSourceOfflineBootstrap(
		context.Background(),
		1,
		f.roster,
		f.authority,
		f,
	)
}

func (f *searchSourceBootstrapScanFixture) roster() ([]uint64, error) {
	f.rosterCalls++
	return []uint64{1}, nil
}

func (f *searchSourceBootstrapScanFixture) authority(channelID string, channelType uint8) (wkdb.ChannelClusterConfig, error) {
	index, ok := f.configIndexes[channelID]
	if !ok || f.configs[index].ChannelType != channelType {
		return wkdb.EmptyChannelClusterConfig, fmt.Errorf("unknown channel %q/%d", channelID, channelType)
	}
	f.authorityCalls++
	if index == len(f.configs)-1 {
		f.finalAuthorityCalls++
	}
	return f.configs[index], nil
}

func (f *searchSourceBootstrapScanFixture) GetChannelClusterConfigs(afterID uint64, limit int) ([]wkdb.ChannelClusterConfig, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("invalid page limit %d", limit)
	}
	f.pageReads++
	start := sort.Search(len(f.configs), func(index int) bool {
		return f.configs[index].Id > afterID
	})
	end := start + limit
	if end > len(f.configs) {
		end = len(f.configs)
	}
	batch := f.configs[start:end]
	if len(batch) > f.maxBatch {
		f.maxBatch = len(batch)
	}
	return batch, nil
}

func (f *searchSourceBootstrapScanFixture) GetChannelClusterConfigRevision() uint64 {
	f.revisionReads++
	return 2
}

func (f *searchSourceBootstrapScanFixture) GetAppliedMsgSeq(channelID string, channelType uint8) (uint64, error) {
	index, ok := f.configIndexes[channelID]
	if !ok || f.configs[index].ChannelType != channelType {
		return 0, fmt.Errorf("unknown channel %q/%d", channelID, channelType)
	}
	f.appliedReads++
	if index == len(f.configs)-1 {
		f.finalAppliedReads++
	}
	return 0, nil
}

func (f *searchSourceBootstrapScanFixture) GetLastMsgSeq(channelID string, channelType uint8) (uint64, error) {
	index, ok := f.configIndexes[channelID]
	if !ok || f.configs[index].ChannelType != channelType {
		return 0, fmt.Errorf("unknown channel %q/%d", channelID, channelType)
	}
	f.physicalReads++
	if index == len(f.configs)-1 {
		f.finalPhysicalReads++
	}
	return 0, nil
}

func (f *searchSourceBootstrapScanFixture) UpdateAppliedMsgSeq(string, uint8, uint64) error {
	return nil
}

func BenchmarkRunSearchSourceOfflineBootstrap60000(b *testing.B) {
	fixture := newSearchSourceBootstrapScanFixture(searchSourceBootstrapBenchmarkChannelCount)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		fixture.resetMetrics()
		if err := fixture.run(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(fixture.pageReads), "page-reads/op")
	b.ReportMetric(float64(fixture.maxBatch), "max-batch")
}
