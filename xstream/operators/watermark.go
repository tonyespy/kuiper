package operators

import (
	"context"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/nodes"
	"math"
	"sort"
	"time"
)

type WatermarkTuple struct {
	Timestamp int64
}

func (t *WatermarkTuple) GetTimestamp() int64 {
	return t.Timestamp
}

func (t *WatermarkTuple) IsWatermark() bool {
	return true
}

type WatermarkGenerator struct {
	lastWatermarkTs int64
	inputTopics     []string
	topicToTs       map[string]int64
	window          *WindowConfig
	lateTolerance   int64
	interval        int
	length          int
	ticker          common.Ticker
	stream          chan<- *WatermarkTuple
}

func NewWatermarkGenerator(window *WindowConfig, l int64, s []string, stream chan<- *WatermarkTuple) (*WatermarkGenerator, error) {
	w := &WatermarkGenerator{
		window:        window,
		topicToTs:     make(map[string]int64),
		lateTolerance: l,
		inputTopics:   s,
		stream:        stream,
	}
	//Tickers to update watermark
	switch window.Type {
	case xsql.TUMBLING_WINDOW:
		w.ticker = common.GetTicker(window.Length)
		w.interval = window.Length
	case xsql.HOPPING_WINDOW:
		w.ticker = common.GetTicker(window.Interval)
		w.interval = window.Interval
		w.length = window.Length
	case xsql.SLIDING_WINDOW:
		w.interval = window.Length
	case xsql.SESSION_WINDOW:
		//Use timeout to update watermark
		w.ticker = common.GetTicker(window.Interval)
		w.interval = window.Interval
	default:
		return nil, fmt.Errorf("unsupported window type %d", window.Type)
	}
	return w, nil
}

func (w *WatermarkGenerator) track(s string, ts int64, ctx api.StreamContext) {
	log := ctx.GetLogger()
	log.Infof("watermark generator track event from topic %s at %d", s, ts)
	currentVal, ok := w.topicToTs[s]
	if !ok || ts > currentVal {
		w.topicToTs[s] = ts
	}
}

func (w *WatermarkGenerator) start(ctx api.StreamContext) {
	log := ctx.GetLogger()
	var c <-chan time.Time

	if w.ticker != nil {
		c = w.ticker.GetC()
	}
	for {
		select {
		case <-c:
			w.trigger(ctx)
		case <-ctx.Done():
			log.Infoln("Cancelling watermark generator....")
			if w.ticker != nil {
				w.ticker.Stop()
			}
			return
		}
	}
}

func (w *WatermarkGenerator) trigger(ctx api.StreamContext) {
	log := ctx.GetLogger()
	watermark := w.computeWatermarkTs(ctx)
	log.Infof("compute watermark event at %d with last %d", watermark, w.lastWatermarkTs)
	if watermark > w.lastWatermarkTs {
		t := &WatermarkTuple{Timestamp: watermark}
		select {
		case w.stream <- t:
		default: //TODO need to set buffer
		}
		w.lastWatermarkTs = watermark
		log.Infof("scan watermark event at %d", watermark)
	}
}

func (w *WatermarkGenerator) computeWatermarkTs(ctx context.Context) int64 {
	var ts int64
	if len(w.topicToTs) >= len(w.inputTopics) {
		ts = math.MaxInt64
		for _, key := range w.inputTopics {
			if ts > w.topicToTs[key] {
				ts = w.topicToTs[key]
			}
		}
	}
	return ts - w.lateTolerance
}

//If window end cannot be determined yet, return max int64 so that it can be recalculated for the next watermark
func (w *WatermarkGenerator) getNextWindow(inputs []*xsql.Tuple, current int64, watermark int64) int64 {
	switch w.window.Type {
	case xsql.TUMBLING_WINDOW:
		interval := int64(w.interval)
		common.Log.Infof("get earliest between %d and %d", current, watermark)
		nextTs := getEarliestEventTs(inputs, current, watermark)
		if nextTs == math.MaxInt64 || nextTs%interval == 0 {
			return nextTs
		}
		return nextTs + (interval - nextTs%interval)
	case xsql.HOPPING_WINDOW:
		interval := int64(w.interval)
		common.Log.Infof("get earliest between %d and %d", current, watermark)
		nextTs := getEarliestEventTs(inputs, current - int64(w.length - w.interval), watermark)
		if nextTs == math.MaxInt64 || nextTs%interval == 0 {
			return nextTs
		}
		nextEnd := nextTs + (interval - nextTs%interval)
		if nextEnd == current{
			nextEnd += interval
		}
		return nextEnd
	case xsql.SLIDING_WINDOW:
		nextTs := getEarliestEventTs(inputs, current, watermark)
		return nextTs
	case xsql.SESSION_WINDOW:
		if len(inputs) > 0 {
			timeout, duration := int64(w.window.Interval), int64(w.window.Length)
			sort.SliceStable(inputs, func(i, j int) bool {
				return inputs[i].Timestamp < inputs[j].Timestamp
			})
			et := inputs[0].Timestamp
			tick := et + (duration - et%duration)
			if et%duration == 0 {
				tick = et
			}
			var p int64
			for _, tuple := range inputs {
				var r int64 = math.MaxInt64
				if p > 0 {
					if tuple.Timestamp-p > timeout {
						r = p + timeout
					}
				}
				if tuple.Timestamp > tick {
					if tick-duration > et && tick < r {
						r = tick
					}
					tick += duration
				}
				if r < math.MaxInt64 {
					return r
				}
				p = tuple.Timestamp
			}
		}
		return math.MaxInt64
	default:
		return math.MaxInt64
	}
}

func (o *WindowOperator) execEventWindow(ctx api.StreamContext, errCh chan<- error) {
	exeCtx, cancel := ctx.WithCancel()
	log := ctx.GetLogger()
	go o.watermarkGenerator.start(exeCtx)
	var (
		nextWindowEndTs int64
		prevWindowEndTs int64
		incomplete    []int64
	)
	packs := make(map[int64][][]*xsql.Tuple)
	for {
		select {
		case d := <- o.watermarkCh:
			watermarkTs := d.GetTimestamp()
			o.sync(WATERMARK, watermarkTs)
			windowEndTs := nextWindowEndTs
			//Get snapshot of inputs
			var inputs []*xsql.Tuple
			for _, worker := range o.workers{
				inputs = append(inputs, worker.Inputs...)
			}
			//Session window needs a recalculation of window because its window end depends the inputs
			if windowEndTs == math.MaxInt64 || o.window.Type == xsql.SESSION_WINDOW || o.window.Type == xsql.SLIDING_WINDOW {
				windowEndTs = o.watermarkGenerator.getNextWindow(inputs, prevWindowEndTs, watermarkTs)
			}
			for windowEndTs <= watermarkTs && windowEndTs >= 0 {
				log.Infof("Window end ts %d Watermark ts %d", windowEndTs, watermarkTs)
				log.Infof("Current input count %d", len(inputs))
				//scan all events and find out the event in the current window
				o.sync(END, windowEndTs)
				prevWindowEndTs = windowEndTs
				windowEndTs = o.watermarkGenerator.getNextWindow(inputs, windowEndTs, watermarkTs)
			}
			nextWindowEndTs = windowEndTs
			log.Infof("next window end %d", nextWindowEndTs)
		case s := <-o.winSigCh:
			switch s.signal{
			case SYNC:
				log.Infof("run sync at %d", s.triggerTime)
				o.sync(END, s.triggerTime)
			case TRACK:
				o.watermarkGenerator.track(s.topic, s.triggerTime, ctx)
				switch o.window.Type {
				case xsql.SLIDING_WINDOW:
					o.sync(END, s.triggerTime)
				}
			}
		case pack := <- o.winCh:
			//TODO treat concurrency one faster
			if ps, ok := packs[pack.triggerTime]; !ok{
				packs[pack.triggerTime] = [][]*xsql.Tuple{
					pack.records,
				}
				i := sort.Search(len(incomplete), func(i int) bool { return incomplete[i] >= pack.triggerTime })
				incomplete = append(incomplete, 0)
				copy(incomplete[i+1:], incomplete[i:])
				incomplete[i] = pack.triggerTime
			}else{
				packs[pack.triggerTime] = append(ps, pack.records)
			}
			for len(incomplete) > 0 && len(packs[incomplete[0]]) == o.concurrency{
				key := incomplete[0]
				var results xsql.WindowTuplesSet = make([]xsql.WindowTuples, 0)
				for _, rs := range packs[key]{
					for _, r := range rs {
						results = results.AddTuple(r)
					}
				}
				results.Sort()
				nodes.Broadcast(o.outputs, results, ctx)
				incomplete = incomplete[1:]
				delete(packs, key)
			}
		// is cancelling
		case <-ctx.Done():
			log.Infoln("Cancelling window....")
			cancel()
			return
		}
	}
}

func getEarliestEventTs(inputs []*xsql.Tuple, startTs int64, endTs int64) int64 {
	var minTs int64 = math.MaxInt64
	for _, t := range inputs {
		if t.Timestamp > startTs && t.Timestamp <= endTs && t.Timestamp < minTs {
			minTs = t.Timestamp
		}
	}
	return minTs
}
