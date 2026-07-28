package channel

import (
	"errors"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"go.uber.org/zap"
)

const (
	searchAppliedObservationQueueSize = 4096
	searchAppliedRetryCapacity        = 4096
	searchAppliedBatchSize            = 256
	searchAppliedRetryTick            = 100 * time.Millisecond
	searchAppliedRetryMinBackoff      = 100 * time.Millisecond
	searchAppliedRetryMaxBackoff      = 5 * time.Second
	searchAppliedWarnInterval         = 5 * time.Second
	searchAppliedStopDrainTimeout     = 3 * time.Second
)

var (
	errSearchAppliedObserverClosed    = errors.New("search applied observer is closed")
	errSearchAppliedRetryCapacity     = errors.New("search applied retry state is full")
	errSearchAppliedObserverUnhealthy = errors.New("search applied observer is unhealthy")
)

type searchAppliedObservation struct {
	key   string
	index uint64
}

// searchAppliedObserver keeps search metadata off the Raft apply path. The
// primary queue and retry/coalescing state are bounded. Failed writes use an
// exponential retry schedule. If bounded state is exhausted, this process
// remains fail-closed; only the next startup's consumed-marker reconciliation
// may repair the dropped tail under roster and authority fencing.
type searchAppliedObserver struct {
	db wkdb.DB
	wklog.Log

	queue     chan searchAppliedObservation
	retryWake chan struct{}
	stopCh    chan struct{}
	doneCh    chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once

	mu              sync.Mutex
	accepting       bool
	retry           map[string]uint64
	overflowed      bool
	retryBackoff    time.Duration
	nextRetryAt     time.Time
	lastWarn        time.Time
	suppressedWarns uint64
}

func newSearchAppliedObserver(db wkdb.DB) *searchAppliedObserver {
	return &searchAppliedObserver{
		db: db, Log: wklog.NewWKLog("channel.searchAppliedObserver"),
		queue:     make(chan searchAppliedObservation, searchAppliedObservationQueueSize),
		retryWake: make(chan struct{}, 1), stopCh: make(chan struct{}), doneCh: make(chan struct{}),
		accepting: true, retry: make(map[string]uint64),
	}
}

func (o *searchAppliedObserver) Start() {
	o.startOnce.Do(func() { go o.run() })
}

// Stop establishes a strict acceptance boundary under o.mu. Every Observe
// that won the lock before Stop is either queued or represented by retry
// state; later calls return errSearchAppliedObserverClosed.
func (o *searchAppliedObserver) Stop() {
	o.stopOnce.Do(func() {
		o.mu.Lock()
		o.accepting = false
		o.mu.Unlock()
		o.Start()
		close(o.stopCh)
	})
	<-o.doneCh
}

func (o *searchAppliedObserver) Observe(key string, index uint64) error {
	if key == "" || index == 0 {
		return errors.New("invalid search applied observation")
	}
	if !supportsSearchAppliedObservation(key) {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.accepting {
		return errSearchAppliedObserverClosed
	}
	observation := searchAppliedObservation{key: key, index: index}
	select {
	case o.queue <- observation:
		return nil
	default:
		wasEmpty := len(o.retry) == 0
		if err := o.addRetryLocked(observation); err != nil {
			o.wakeRetry()
			return err
		}
		if wasEmpty {
			o.wakeRetry()
		}
		// Successfully coalescing into bounded retry state is accepted work,
		// not a Raft warning for every message.
		return nil
	}
}

func supportsSearchAppliedObservation(channelKey string) bool {
	channelID, channelType := wkutil.ChannelFromlKey(channelKey)
	return key.IsValidSearchOutboxChannelIdentity(channelID, channelType)
}

func (o *searchAppliedObserver) Ready() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.accepting || len(o.retry) != 0 || o.overflowed {
		return errSearchAppliedObserverUnhealthy
	}
	return nil
}

func (o *searchAppliedObserver) run() {
	defer close(o.doneCh)
	ticker := time.NewTicker(searchAppliedRetryTick)
	defer ticker.Stop()
	for {
		select {
		case <-o.stopCh:
			o.drainOnStop(time.Now().Add(searchAppliedStopDrainTimeout))
			return
		default:
		}
		select {
		case first := <-o.queue:
			batch := o.queueBatch(first)
			if o.hasRecoveryWork() {
				o.deferBatch(batch)
			} else {
				o.finishAttempt(o.flush(batch))
			}
		case <-o.retryWake:
			o.attemptRecovery(false)
		case <-ticker.C:
			o.attemptRecovery(false)
		case <-o.stopCh:
			o.drainOnStop(time.Now().Add(searchAppliedStopDrainTimeout))
			return
		}
	}
}

func (o *searchAppliedObserver) queueBatch(first searchAppliedObservation) map[string]uint64 {
	pending := map[string]uint64{first.key: first.index}
	for consumed := 1; consumed < searchAppliedBatchSize; consumed++ {
		select {
		case observation := <-o.queue:
			if observation.index > pending[observation.key] {
				pending[observation.key] = observation.index
			}
		default:
			return pending
		}
	}
	return pending
}

func (o *searchAppliedObserver) deferBatch(pending map[string]uint64) {
	o.mu.Lock()
	for key, index := range pending {
		_ = o.addRetryLocked(searchAppliedObservation{key: key, index: index})
	}
	o.mu.Unlock()
}

func (o *searchAppliedObserver) retrySnapshot() map[string]uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	pending := make(map[string]uint64, min(len(o.retry), searchAppliedBatchSize))
	for key, index := range o.retry {
		pending[key] = index
		if len(pending) == searchAppliedBatchSize {
			break
		}
	}
	return pending
}

// flush stops after the first storage failure and defers all remaining work,
// preventing a database outage from multiplying into thousands of writes and
// warnings per retry tick.
func (o *searchAppliedObserver) flush(pending map[string]uint64) error {
	for channelKey, index := range pending {
		channelID, channelType := wkutil.ChannelFromlKey(channelKey)
		if !key.IsValidSearchOutboxChannelIdentity(channelID, channelType) {
			o.recordSuccess(channelKey, index)
			delete(pending, channelKey)
			continue
		}
		if err := o.db.UpdateChannelAppliedIndex(channelID, channelType, index); err != nil {
			o.deferBatch(pending)
			return err
		}
		o.recordSuccess(channelKey, index)
		delete(pending, channelKey)
	}
	return nil
}

func (o *searchAppliedObserver) recordSuccess(key string, index uint64) {
	o.mu.Lock()
	if retryIndex, ok := o.retry[key]; ok && retryIndex <= index {
		delete(o.retry, key)
	}
	o.mu.Unlock()
}

func (o *searchAppliedObserver) addRetryLocked(observation searchAppliedObservation) error {
	if current, ok := o.retry[observation.key]; ok {
		if observation.index > current {
			o.retry[observation.key] = observation.index
		}
		return nil
	}
	if len(o.retry) >= searchAppliedRetryCapacity {
		// The observation cannot be represented in bounded memory. Keep the
		// whole search source fail-closed for this process. A later startup may
		// repair it only through the consumed-marker, roster-fenced recovery.
		o.overflowed = true
		return errSearchAppliedRetryCapacity
	}
	o.retry[observation.key] = observation.index
	return nil
}

func (o *searchAppliedObserver) hasRecoveryWork() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.retry) != 0
}

func (o *searchAppliedObserver) attemptRecovery(ignoreBackoff bool) {
	o.mu.Lock()
	if !ignoreBackoff && !o.nextRetryAt.IsZero() && time.Now().Before(o.nextRetryAt) {
		o.mu.Unlock()
		return
	}
	o.mu.Unlock()

	pending := o.retrySnapshot()
	if len(pending) == 0 {
		return
	}
	err := o.flush(pending)
	o.finishAttempt(err)
}

func (o *searchAppliedObserver) finishAttempt(err error) {
	o.mu.Lock()
	if err != nil {
		if o.retryBackoff == 0 {
			o.retryBackoff = searchAppliedRetryMinBackoff
		} else {
			o.retryBackoff *= 2
			if o.retryBackoff > searchAppliedRetryMaxBackoff {
				o.retryBackoff = searchAppliedRetryMaxBackoff
			}
		}
		o.nextRetryAt = time.Now().Add(o.retryBackoff)
	} else if len(o.retry) == 0 {
		o.retryBackoff = 0
		o.nextRetryAt = time.Time{}
	} else {
		o.retryBackoff = searchAppliedRetryMinBackoff
		o.nextRetryAt = time.Now().Add(o.retryBackoff)
	}
	o.mu.Unlock()
	if err != nil {
		o.warnRetry(err)
	}
}

func (o *searchAppliedObserver) warnRetry(err error) {
	now := time.Now()
	o.mu.Lock()
	if !o.lastWarn.IsZero() && now.Sub(o.lastWarn) < searchAppliedWarnInterval {
		o.suppressedWarns++
		o.mu.Unlock()
		return
	}
	suppressed := o.suppressedWarns
	o.suppressedWarns = 0
	o.lastWarn = now
	o.mu.Unlock()
	o.Warn("persist search applied watermark failed; search source remains disabled",
		zap.Error(err), zap.Uint64("suppressed", suppressed))
}

func (o *searchAppliedObserver) wakeRetry() {
	select {
	case o.retryWake <- struct{}{}:
	default:
	}
}

func (o *searchAppliedObserver) drainOnStop(deadline time.Time) {
	for {
		select {
		case first := <-o.queue:
			o.deferBatch(o.queueBatch(first))
			continue
		default:
		}
		if !o.hasRecoveryWork() {
			return
		}
		if time.Now().After(deadline) {
			o.warnRetry(errors.New("search applied observer shutdown drain timed out"))
			return
		}
		o.attemptRecovery(true)
		if o.hasRecoveryWork() {
			time.Sleep(searchAppliedRetryMinBackoff)
		}
	}
}
