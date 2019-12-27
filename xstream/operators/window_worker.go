package operators

import (
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/nodes"
	"github.com/prometheus/common/log"
	"math"
	"time"
)

type windowWorker struct {
	Inputs        []*xsql.Tuple
	SigCh        chan *scanSignal       //The channel to receive timing event from the window op
	Stats        *nodes.StatManager
	
	inputCh		 <-chan interface{}    //The channel to receive inputs
	winCh        chan<- *windowPack  //The channel to send out window results
	winSigCh	 chan<- *scanSignal    //The channel to send signal to the window op
	
	ctx          api.StreamContext
	window       *WindowConfig
	triggerTime  int64
	isEventTime  bool
	watermark    int64
}

func newWindowWorker(window *WindowConfig, inputCh chan interface{}, winCh chan *windowPack, winSigCh chan *scanSignal, isEventTime bool, ctx api.StreamContext) (*windowWorker, error){
	if stats, err := nodes.NewStatManager("op", ctx); err != nil{
		return nil, err
	}else{
		return &windowWorker{
			SigCh:  make(chan *scanSignal, 100),
			ctx:    ctx,
			Stats:  stats,
			window: window,
			inputCh:inputCh,
			winCh:  winCh,
			winSigCh:winSigCh,
			isEventTime:isEventTime,
		}, nil
	}	
}

func (o *windowWorker) run() {
	for {
		select {
		case item, opened := <-o.inputCh:
			o.Stats.IncTotalRecordsIn()
			if !opened {
				o.Stats.IncTotalExceptions()
				break
			}
			if d, ok := item.(*xsql.Tuple); !ok {
				log.Errorf("Expect xsql.Tuple type")
				o.Stats.IncTotalExceptions()
				break
			} else {
				if d.GetTimestamp() < o.watermark{
					log.Warnf("Receive late event %s at %d for watermark %d", d.Message, d.GetTimestamp(), o.watermark)
					break
				}
				log.Infof("Event window receive tuple %s", d.Message)
				o.Inputs = append(o.Inputs, d)
				if o.isEventTime{
					o.winSigCh <- &scanSignal{signal:TRACK, triggerTime: d.Timestamp, topic: d.Emitter}
				}else{
					switch o.window.Type {
					case xsql.SLIDING_WINDOW:
						log.Infof("send sync with time %d", d.Timestamp)
						o.winSigCh <- &scanSignal{signal:SYNC, triggerTime: d.Timestamp}
					case xsql.SESSION_WINDOW: //when doing session window, the signal is a channel with 1 buffer. Only 1 signal is allowed
						log.Infof("send session with time %d", d.Timestamp)
						o.winSigCh <- &scanSignal{signal:SESSION, triggerTime: d.Timestamp}
					}
				}
			}
		case sig := <- o.SigCh:
			switch sig.signal {
			case END:
				if len(o.Inputs) > 0 {
					o.Stats.ProcessTimeStart()
					log.Infof("trigger instance %d at %d with %d inputs", o.ctx.GetInstanceId(), sig.triggerTime, len(o.Inputs))
					o.Inputs = o.scan(o.Inputs, sig.triggerTime, o.ctx)
					log.Infof("scan done for instance %d", o.ctx.GetInstanceId())
					o.Stats.ProcessTimeEnd()
				}
			case TIMEOUT:
				if len(o.Inputs) > 0 {
					o.Stats.ProcessTimeStart()
					log.Infof("triggered by timeout")
					o.Inputs = o.scan(o.Inputs, sig.triggerTime, o.ctx)
					o.Stats.ProcessTimeEnd()
				}
			case WATERMARK:
				o.watermark = sig.triggerTime
			}
		// is cancelling
		case <- o.ctx.Done():
			log.Infoln("Cancelling window op instance....", o.ctx.GetInstanceId())
			return
		}
	}
}

func (o *windowWorker) scan(inputs []*xsql.Tuple, triggerTime int64, ctx api.StreamContext) []*xsql.Tuple {
	log := ctx.GetLogger()
	log.Infof("window %s instance %d triggered at %s", ctx.GetOpId(), ctx.GetInstanceId(), time.Unix(triggerTime/1000, triggerTime%1000))
	var delta int64
	if o.window.Type == xsql.HOPPING_WINDOW || o.window.Type == xsql.SLIDING_WINDOW {
		delta = o.calDelta(triggerTime, delta, log)
	}
	var results []*xsql.Tuple
	i := 0
	//Sync table
	for _, tuple := range inputs {
		if o.window.Type == xsql.HOPPING_WINDOW || o.window.Type == xsql.SLIDING_WINDOW {
			diff := o.triggerTime - tuple.Timestamp
			if diff > int64(o.window.Length)+delta {
				log.Infof("diff: %d, length: %d, delta: %d", diff, o.window.Length, delta)
				log.Infof("tuple %s emitted at %d expired", tuple, tuple.Timestamp)
				//Expired tuple, remove it by not adding back to inputs
				continue
			}
			//Added back all inputs for non expired events
			inputs[i] = tuple
			i++
		} else if tuple.Timestamp > triggerTime {
			//Only added back early arrived events
			inputs[i] = tuple
			i++
		}
		if tuple.Timestamp <= triggerTime {
			log.Infof("added a tuple %d <= %d", tuple.Timestamp, triggerTime)
			results = append(results, tuple)
		}
	}

	if len(results) > 0 {
		log.Infof("window %s instance %d triggered for %d tuples", ctx.GetOpId(), ctx.GetInstanceId(), len(results))
		o.winCh <- &windowPack{triggerTime:triggerTime, records: results}
		o.Stats.IncTotalRecordsOut()
		log.Infof("done scan")
	}

	return inputs[:i]
}

func (o *windowWorker) calDelta(triggerTime int64, delta int64, log api.Logger) int64 {
	lastTriggerTime := o.triggerTime
	o.triggerTime = triggerTime
	if lastTriggerTime <= 0 {
		delta = math.MaxInt16 //max int, all events for the initial window
	} else {
		if !o.isEventTime && o.window.Interval > 0 {
			delta = o.triggerTime - lastTriggerTime - int64(o.window.Interval)
			if delta > 100 {
				log.Warnf("Possible long computation in window; Previous eviction time: %d, current eviction time: %d", lastTriggerTime, o.triggerTime)
			}
		} else {
			delta = 0
		}
	}
	return delta
}


