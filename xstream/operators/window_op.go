package operators

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/nodes"
	"github.com/prometheus/common/log"
	"sort"
	"sync"
	"time"
)

type WindowConfig struct {
	Type     xsql.WindowType
	Length   int
	Interval int //If interval is not set, it is equals to Length
}

type signal int
const (
	SYNC signal = iota
	SESSION
	END
	TIMEOUT
	WATERMARK
	TRACK
)

type scanSignal struct{
	signal signal
	triggerTime int64
	topic	string
}

type windowPack struct {
	records []*xsql.Tuple
	triggerTime int64
}

type WindowOperator struct {
	input              chan interface{}
	watermarkCh        chan *WatermarkTuple
	outputs            map[string]chan<- interface{}
	name               string
	ticker             common.Ticker //For processing time only
	window             *WindowConfig
	interval           int
	triggerTime		   int64
	isEventTime        bool
	watermarkGenerator *WatermarkGenerator //For event time only

	concurrency        int
	mutex       	   sync.RWMutex
	winCh        	   chan *windowPack  //The channel to receive window results from workers
	winSigCh	 	   chan *scanSignal  //The channel to receive signal from workers
	workers            []*windowWorker
}

func NewWindowOp(name string, w *xsql.Window, isEventTime bool, lateTolerance int64, streams []string) (*WindowOperator, error) {
	o := new(WindowOperator)

	o.input = make(chan interface{}, 1024)
	o.outputs = make(map[string]chan<- interface{})
	o.name = name
	o.concurrency = 1
	o.isEventTime = isEventTime
	o.winCh  = make(chan *windowPack)
	o.winSigCh = make(chan *scanSignal)
	if w != nil {
		o.window = &WindowConfig{
			Type:     w.WindowType,
			Length:   w.Length.Val,
			Interval: w.Interval.Val,
		}
	}

	if isEventTime {
		o.watermarkCh = make(chan *WatermarkTuple, 1024)
		//Create watermark generator
		if w, err := NewWatermarkGenerator(o.window, lateTolerance, streams, o.watermarkCh); err != nil {
			return nil, err
		} else {
			o.watermarkGenerator = w
		}
	} else {
		switch o.window.Type {
		case xsql.TUMBLING_WINDOW:
			o.ticker = common.GetTicker(o.window.Length)
			o.interval = o.window.Length
		case xsql.HOPPING_WINDOW:
			o.ticker = common.GetTicker(o.window.Interval)
			o.interval = o.window.Interval
		case xsql.SLIDING_WINDOW:
			o.interval = o.window.Length
		case xsql.SESSION_WINDOW:
			o.ticker = common.GetTicker(o.window.Length)
			o.interval = o.window.Interval
		default:
			return nil, fmt.Errorf("unsupported window type %d", o.window.Type)
		}
	}
	return o, nil
}

func (o *WindowOperator) SetConcurrency(concurr int) {
	o.concurrency = concurr
	if o.concurrency < 1 {
		o.concurrency = 1
	}
}

func (o *WindowOperator) GetName() string {
	return o.name
}

func (o *WindowOperator) AddOutput(output chan<- interface{}, name string) error {
	if _, ok := o.outputs[name]; !ok {
		o.outputs[name] = output
	} else {
		return fmt.Errorf("fail to add output %s, operator %s already has an output of the same name", name, o.name)
	}
	return nil
}

func (o *WindowOperator) GetInput() (chan<- interface{}, string) {
	return o.input, o.name
}

// Exec is the entry point for the executor
// input: *xsql.Tuple from preprocessor
// output: xsql.WindowTuplesSet
func (o *WindowOperator) Exec(ctx api.StreamContext, errCh chan<- error) {
	logger := ctx.GetLogger()
	logger.Infof("Window operator %s is started", o.name)

	if len(o.outputs) <= 0 {
		go func() { errCh <- fmt.Errorf("no output channel found") }()
		return
	}
	if o.concurrency < 1 {
		o.concurrency = 1
	}
	logger.Debugf("Window operator %s is started with concurrency %d", o.name, o.concurrency)
	//Starting thw workers
	for i := 0; i < o.concurrency; i++ { // workers
		w, err := newWindowWorker(o.window, o.input, o.winCh, o.winSigCh, o.isEventTime, ctx.WithInstance(i))
		if err != nil {
			go func() { errCh <- err }()
			return
		}
		o.workers = append(o.workers, w)
		go w.run()
	}
	if o.isEventTime {
		go o.execEventWindow(ctx, errCh)
	} else {
		go o.execProcessingWindow(ctx, errCh)
	}
}

func (o *WindowOperator) execProcessingWindow(ctx api.StreamContext, errCh chan<- error) {
	//create the op loop for issue time events and receive results
	var(
		c             <-chan time.Time
		packs		  map[int64][][]*xsql.Tuple
		incomplete    []int64
		last		  int64
		timeoutTicker common.Timer
		timeout       <-chan time.Time
	)
	if o.ticker != nil {
		c = o.ticker.GetC()
	}
	packs = make(map[int64][][]*xsql.Tuple)
	for{
		select{
		//receive window length event from tumble,hopping or session
		case now := <-c:
			n := common.TimeToUnixMilli(now)
			log.Infof("receive tick %d", n)
			//For session window, check if the last scan time is newer than the inputs
			if o.window.Type == xsql.SESSION_WINDOW {
				//scan time for session window will record all triggers of the ticker but not the timeout
				lastTriggerTime := o.triggerTime
				o.triggerTime = n
				var minT int64 = -1
				for _, w := range o.workers{
					if len(w.Inputs) > 0 && (minT < 0 || minT > w.Inputs[0].Timestamp){
						minT = w.Inputs[0].Timestamp
					}
				}
				log.Infof("current minimum record %d and the lastTriggerTime %d", minT, lastTriggerTime)
				//Check if the current window has exceeded the max duration, if not continue expand
				if lastTriggerTime < minT {
					break
				}
			}
			log.Infof("triggered by ticker")
			o.sync(END, n)
		//From sliding window/session window
		case s := <-o.winSigCh:
			switch s.signal{
			case SYNC:
				log.Infof("run sync at %d", s.triggerTime)
				o.sync(END, s.triggerTime)
			case SESSION:
				if timeoutTicker != nil {
					if s.triggerTime > last{
						timeoutTicker.Stop()
						timeoutTicker.Reset(time.Duration(o.window.Interval) * time.Millisecond)
					}
				} else {
					timeoutTicker = common.GetTimer(o.window.Interval)
					timeout = timeoutTicker.GetC()
				}
				last = s.triggerTime
			}
		case now := <-timeout:
			log.Infof("triggered by timeout")
			o.sync(TIMEOUT, common.TimeToUnixMilli(now))
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
				//if o.concurrency > 1{
				//	results.Sort()
				//}
				nodes.Broadcast(o.outputs, results, ctx)
				incomplete = incomplete[1:]
				delete(packs, key)
			}
		case <-ctx.Done():
			log.Infoln("Cancelling window....")
			if o.ticker != nil {
				o.ticker.Stop()
			}
			return
		}
	}
}

func (o *WindowOperator) sync(signal signal, triggerTime int64){
	s := &scanSignal{
		signal:      signal,
		triggerTime: triggerTime,
	}
	for _, w := range o.workers{
		w.SigCh <- s
	}
}

func (o *WindowOperator) GetMetrics() map[string]interface{} {
	result := make(map[string]interface{})
	for _, w := range o.workers{
		for k, v := range w.Stats.GetMetrics(){
			result[k] = v
		}
	}
	return result

}
