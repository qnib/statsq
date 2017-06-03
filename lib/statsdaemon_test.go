package statsdaemon

import (
	"bytes"
	"flag"
	//"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/zpatrick/go-config"

	"github.com/qnib/qframe-types"
	"github.com/stretchr/testify/assert"
	"fmt"
)

func NewCfg() *config.Config {
	return NewPreCfg(map[string]string{})
}

func NewPreCfg(pre map[string]string) *config.Config {
	cfgMap := map[string]string{
		"log.level":                 "info",
		"test":               "0",
		"address":            ":8125",
		"debug":              "false",
		"resent-gauges":      "false",
		"persist-count-keys": "60",
	}
	for k, v := range pre {
		cfgMap[k] = v
	}
	cfg := config.NewConfig(
		[]config.Provider{
			config.NewStatic(cfgMap),
		},
	)
	return cfg
}

func TestNewStatsdaemonPercentiles(t *testing.T) {
	pre := map[string]string{"percentiles": "0.9"}
	cfg := NewPreCfg(pre)
	sd := NewStatsdaemon(cfg)
	assert.Equal(t, "[0_9]", sd.Percentiles.String())
	pre = map[string]string{"percentiles": "0.9,0.95"}
	cfg = NewPreCfg(pre)
	sd = NewStatsdaemon(cfg)
	assert.Equal(t, "[0_9 0_95]", sd.Percentiles.String())
}

func TestProcessGauges(t *testing.T) {
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)

	var buffer bytes.Buffer
	now := int64(1418052649)

	sd.Gauges["gorets"] = float64(123)
	num := sd.ProcessGauges(&buffer, now)
	assert.Equal(t, num, int64(1))
	assert.Equal(t, buffer.String(), "gorets 123 1418052649\n")

	lines := bytes.Split(buffer.Bytes(), []byte("\n"))
	// expect two more lines - the good one and an empty one at the end
	assert.Equal(t, "gorets 123 1418052649", string(lines[0]))
}

func TestProcessCounters(t *testing.T) {
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)

	var buffer bytes.Buffer
	now := int64(1418052649)

	sd.Counters["gorets"] = float64(123)

	num := sd.ProcessCounters(&buffer, now)
	assert.Equal(t, num, int64(1))
	assert.Equal(t, buffer.String(), "gorets 123 1418052649\n")

	// run processCounters() enough times to make sure it purges items
	for i := 0; i < sd.Int("persist-count-keys")+10; i++ {
		num = sd.ProcessCounters(&buffer, now)
	}
	lines := bytes.Split(buffer.Bytes(), []byte("\n"))

	// expect two more lines - the good one and an empty one at the end
	assert.Equal(t, sd.Int("persist-count-keys")+2, len(lines))
	assert.Equal(t, "gorets 123 1418052649", string(lines[0]))
	assert.Equal(t, "gorets 0 1418052649", string(lines[sd.Int("persist-count-keys")]))
}

func TestProcessSets(t *testing.T) {
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)
	now := int64(1418052649)

	var buffer bytes.Buffer

	// three unique values
	sd.Sets["uniques"] = []string{"123", "234", "345"}
	num := sd.ProcessSets(&buffer, now)
	assert.Equal(t, num, int64(1))
	assert.Equal(t, buffer.String(), "uniques 3 1418052649\n")

	// one value is repeated
	buffer.Reset()
	sd.Sets["uniques"] = []string{"123", "234", "234"}
	num = sd.ProcessSets(&buffer, now)
	assert.Equal(t, num, int64(1))
	assert.Equal(t, buffer.String(), "uniques 2 1418052649\n")

	// make sure sets are purged
	num = sd.ProcessSets(&buffer, now)
	assert.Equal(t, num, int64(0))
}

func TestProcessTimers(t *testing.T) {
	// Some data with expected mean of 20
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)

	sd.Timers["response_time"] = []float64{0, 30, 30}

	now := int64(1418052649)

	var buffer bytes.Buffer
	num := sd.ProcessTimers(&buffer, now)

	lines := bytes.Split(buffer.Bytes(), []byte("\n"))

	assert.Equal(t, num, int64(1))
	assert.Equal(t, string(lines[0]), "response_time.mean 20 1418052649")
	assert.Equal(t, string(lines[1]), "response_time.upper 30 1418052649")
	assert.Equal(t, string(lines[2]), "response_time.lower 0 1418052649")
	assert.Equal(t, string(lines[3]), "response_time.count 3 1418052649")

	num = sd.ProcessTimers(&buffer, now)
	assert.Equal(t, num, int64(0))
}

func TestProcessTimersUpperPercentile(t *testing.T) {
	pre := map[string]string{"percentiles": "75"}
	cfg := NewPreCfg(pre)
	sd := NewStatsdaemon(cfg)

	// Some data with expected 75% of 2
	sd.Timers["response_time"] = []float64{0, 1, 2, 3}

	now := int64(1418052649)

	var buffer bytes.Buffer
	num := sd.ProcessTimers(&buffer, now)

	lines := bytes.Split(buffer.Bytes(), []byte("\n"))

	assert.Equal(t, num, int64(1))
	assert.Equal(t, string(lines[0]), "response_time.upper_75 2 1418052649")
}

func TestProcessTimersUpperPercentilePostfix(t *testing.T) {
	pre := map[string]string{
		"postfix":     ".test",
		"percentiles": "75",
	}
	cfg := NewPreCfg(pre)
	sd := NewStatsdaemon(cfg)

	// Some data with expected 75% of 2
	sd.Timers["postfix_response_time.test"] = []float64{0, 1, 2, 3}

	now := int64(1418052649)

	var buffer bytes.Buffer
	num := sd.ProcessTimers(&buffer, now)

	lines := bytes.Split(buffer.Bytes(), []byte("\n"))

	assert.Equal(t, num, int64(1))
	assert.Equal(t, string(lines[0]), "postfix_response_time.upper_75.test 2 1418052649")
	flag.Set("postfix", "")
}

func TestProcessTimesLowerPercentile(t *testing.T) {
	pre := map[string]string{"percentiles": "-75"}
	cfg := NewPreCfg(pre)
	sd := NewStatsdaemon(cfg)
	sd.Timers["time"] = []float64{0, 1, 2, 3}
	now := int64(1418052649)
	var buffer bytes.Buffer
	num := sd.ProcessTimers(&buffer, now)

	lines := bytes.Split(buffer.Bytes(), []byte("\n"))

	assert.Equal(t, num, int64(1))
	assert.Equal(t, "time.lower_75 1 1418052649", string(lines[0]))
}

func TestStatsDaemonParseLine(t *testing.T) {
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)
	gid := GenID("gorets")
	sd.ParseLine("gorets:100|c")
	assert.Equal(t, float64(100), sd.Counters[gid])
	sd.ParseLine("gorets:3|c")
	assert.Equal(t, float64(103), sd.Counters[gid])
	sd.ParseLine("gorets:-4|c")
	assert.Equal(t, float64(99), sd.Counters[gid])
	sd.ParseLine("gorets:-100|c")
	assert.Equal(t, float64(-1), sd.Counters[gid])
	//Gauges
	sd.ParseLine("testGauge:+100|g")
	gid = GenID("testGauge")
	assert.Equal(t, float64(100), sd.Gauges[gid])

}

func TestStatsDaemonFanOutCounters(t *testing.T) {
	cfg := NewCfg()
	qchan := qtypes.NewQChan()
	sd := NewNamedStatsdaemon("", cfg, qchan)
	qchan.Broadcast()
	dc := qchan.Data.Join()
	sd.ParseLine("gorets:100|c")
	sd.ParseLine("gorets:3|c")
	now := time.Unix(1495028544, 0)
	sd.FanOutCounters(now)
	select {
	case val := <-dc.Read:
		assert.IsType(t, qtypes.Metric{}, val)
		met := val.(qtypes.Metric)
		assert.Equal(t, float64(103), met.Value)
		assert.Equal(t, "gorets", met.Name)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("metrics receive timeout")
	}
	sd.FanOutCounters(now)
	select {
	case val := <-dc.Read:
		assert.IsType(t, qtypes.Metric{}, val)
		met := val.(qtypes.Metric)
		assert.Equal(t, float64(0), met.Value)
		assert.Equal(t, "gorets", met.Name)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("metrics receive timeout")
	}

}

func TestStatsDaemonFanOutGauges(t *testing.T) {
	cfg := NewCfg()
	qchan := qtypes.NewQChan()
	sd := NewNamedStatsdaemon("statsd", cfg, qchan)
	qchan.Broadcast()
	dc := qchan.Data.Join()
	sd.ParseLine("testGauge:100|g")
	gid := GenID("testGauge")
	assert.Equal(t, float64(100), sd.Gauges[gid])
	now := time.Unix(1495028544, 0)
	sd.FanOutGauges(now)
	select {
	case val := <-dc.Read:
		assert.IsType(t, qtypes.Metric{}, val)
		met := val.(qtypes.Metric)
		assert.Equal(t, float64(100), met.Value)
		assert.Equal(t, "testGauge", met.Name)
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("metrics receive timeout")
	}
	sd.ParseLine("testGauge:-50|g")
	assert.Equal(t, float64(50), sd.Gauges[gid])
	sd.FanOutGauges(now)
	select {
	case val := <-dc.Read:
		assert.IsType(t, qtypes.Metric{}, val)
		met := val.(qtypes.Metric)
		assert.Equal(t, float64(50), met.Value)
		assert.Equal(t, "testGauge", met.Name)
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("metrics receive timeout")
	}
	sd.ParseLine("testGauge:10|g")
	assert.Equal(t, float64(10), sd.Gauges[gid])
	sd.ParseLine("testGauge:+10|g")
	assert.Equal(t, float64(20), sd.Gauges[gid])
}

func TestStatsDaemonFanOutGaugesDelete(t *testing.T) {
	pre := map[string]string{"statsd.delete-gauges": "true"}
	cfg := NewPreCfg(pre)
	qchan := qtypes.NewQChan()
	sd := NewNamedStatsdaemon("statsd", cfg, qchan)
	qchan.Broadcast()
	dc := qchan.Data.Join()
	sd.ParseLine("testGauge:100|g")
	gid := GenID("testGauge")
	assert.Equal(t, float64(100), sd.Gauges[gid])
	now := time.Unix(1495028544, 0)
	sd.FanOutGauges(now)
	select {
	case val := <-dc.Read:
		assert.IsType(t, qtypes.Metric{}, val)
		met := val.(qtypes.Metric)
		assert.Equal(t, float64(100), met.Value)
		assert.Equal(t, "testGauge", met.Name)
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("metrics receive timeout")
	}
	sd.ParseLine("testGauge:-50|g")
	assert.Equal(t, float64(0), sd.Gauges[gid])
	sd.FanOutGauges(now)
	select {
	case val := <-dc.Read:
		assert.IsType(t, qtypes.Metric{}, val)
		met := val.(qtypes.Metric)
		assert.Equal(t, float64(0), met.Value)
		assert.Equal(t, "testGauge", met.Name)
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("metrics receive timeout")
	}
}

func TestStatsDaemonFanOutSets(t *testing.T) {
	cfg := NewCfg()
	qchan := qtypes.NewQChan()
	sd := NewNamedStatsdaemon("test", cfg, qchan)
	qchan.Broadcast()
	dc := qchan.Data.Join()
	gid := GenID("testSet")
	sd.ParseLine("testSet:100|s")
	assert.Equal(t, 1, len(sd.Sets[gid]))
	assert.Equal(t, "100", sd.Sets[gid][0])

	now := time.Unix(1495028544, 0)
	sd.FanOutSets(now)
	select {
	case val := <-dc.Read:
		assert.IsType(t, qtypes.Metric{}, val)
		met := val.(qtypes.Metric)
		assert.Equal(t, float64(1), met.Value)
		assert.Equal(t, "testSet", met.Name)
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("metrics receive timeout")
	}
	sd.ParseLine("testSet:100|s")
	sd.ParseLine("testSet:200|s")
	assert.Equal(t, 2, len(sd.Sets[gid]))
	assert.Equal(t, "200", sd.Sets[gid][1])
	sd.FanOutSets(now)
	select {
	case val := <-dc.Read:
		assert.IsType(t, qtypes.Metric{}, val)
		met := val.(qtypes.Metric)
		assert.Equal(t, float64(2), met.Value)
		assert.Equal(t, "testSet", met.Name)
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("metrics receive timeout")
	}
}

func TestStatsDaemonFanOutTimers(t *testing.T) {
	cfg := NewCfg()
	qchan := qtypes.NewQChan()
	sd := NewNamedStatsdaemon("", cfg, qchan)
	qchan.Broadcast()
	dc := qchan.Data.Join()
	sd.ParseLine("testTimer:100|ms")
	gid := GenID("testTimer")
	assert.Equal(t, float64(100), sd.Timers[gid][0])
	now := time.Unix(1495028544, 0)
	sd.FanOutTimers(now)
	exp := map[string]float64{
		"testTimer.upper": 100.0,
		"testTimer.lower": 100.0,
		"testTimer.mean":  100.0,
		"testTimer.count": 1.0,
	}
	tr := NewTimerResult(exp)
	for {
		select {
		case val := <-dc.Read:
			assert.IsType(t, qtypes.Metric{}, val)
			met := val.(qtypes.Metric)
			tr.Input(met.Name, met.Value)
		case <-time.After(1500 * time.Millisecond):
			t.Fatal("timeout")
		}
		if tr.Check() {
			break
		}
	}
	sd.ParseLine("testTimer:100|ms")
	sd.ParseLine("testTimer:200|ms")
	sd.FanOutTimers(now)
	exp = map[string]float64{
		"testTimer.upper": 200.0,
		"testTimer.lower": 100.0,
		"testTimer.mean":  150.0,
		"testTimer.count": 2.0,
	}
	tr = NewTimerResult(exp)
	for {
		select {
		case val := <-dc.Read:
			assert.IsType(t, qtypes.Metric{}, val)
			met := val.(qtypes.Metric)
			tr.Input(met.Name, met.Value)
		case <-time.After(1500 * time.Millisecond):
			t.Fatal("timeout")
		}
		if tr.Check() {
			break
		}
	}
}

func TestStatsDaemonFanOutTimersPercentiles(t *testing.T) {
	pre := map[string]string{"percentiles": "90"}
	cfg := NewPreCfg(pre)
	qchan := qtypes.NewQChan()
	sd := NewNamedStatsdaemon("", cfg, qchan)
	now := time.Unix(1495028544, 0)
	qchan.Broadcast()
	dc := qchan.Data.Join()
	sd.ParseLine("testTimer:100|ms")
	sd.ParseLine("testTimer:200|ms")
	sd.FanOutTimers(now)
	exp := map[string]float64{
		"testTimer.upper":    200.0,
		"testTimer.upper_90": 200.0,
		"testTimer.lower":    100.0,
		"testTimer.mean":     150.0,
		"testTimer.count":    2.0,
	}
	tr := NewTimerResult(exp)
	for {
		select {
		case val := <-dc.Read:
			assert.IsType(t, qtypes.Metric{}, val)
			met := val.(qtypes.Metric)
			tr.Input(met.Name, met.Value)
		case <-time.After(1500 * time.Millisecond):
			fmt.Println(tr.Result())
			t.Fatal("timeout")
		}
		if tr.Check() {
			break
		}
	}
}

func TestStatsDaemonFanOutTimersMorePercentiles(t *testing.T) {
	pre := map[string]string{
		"percentiles": "50,90,95,99",
	}
	cfg := NewPreCfg(pre)
	qchan := qtypes.NewQChan()
	sd := NewNamedStatsdaemon("", cfg, qchan)
	now := time.Unix(1495028544, 0)
	qchan.Broadcast()
	dc := qchan.Data.Join()
	sd.ParseLine("testTimer:80|ms")
	sd.ParseLine("testTimer:80|ms")
	sd.ParseLine("testTimer:80|ms")
	sd.ParseLine("testTimer:80|ms")
	sd.ParseLine("testTimer:80|ms")
	sd.ParseLine("testTimer:80|ms")
	sd.ParseLine("testTimer:80|ms")
	sd.ParseLine("testTimer:80|ms")
	sd.ParseLine("testTimer:100|ms")
	sd.ParseLine("testTimer:100|ms")
	sd.ParseLine("testTimer:100|ms")
	sd.ParseLine("testTimer:100|ms")
	sd.ParseLine("testTimer:100|ms")
	sd.ParseLine("testTimer:200|ms")
	sd.FanOutTimers(now)
	exp := map[string]float64{
		"testTimer.upper":    200.0,
		"testTimer.upper_50": 80.0,
		"testTimer.upper_90": 100.0,
		"testTimer.upper_95": 100.0,
		"testTimer.upper_99": 200.0,
		"testTimer.lower":    80.0,
		"testTimer.mean":     95.71428571428571,
		"testTimer.count":    14.0,
	}
	tr := NewTimerResult(exp)
	for {
		select {
		case val := <-dc.Read:
			assert.IsType(t, qtypes.Metric{}, val)
			met := val.(qtypes.Metric)
			tr.Input(met.Name, met.Value)
		case <-time.After(1500 * time.Millisecond):
			fmt.Printf(tr.Result())
			t.Fatal("timeout")
		}
		if tr.Check() {
			break
		}
	}
}


/******************* StatsdPackets
Handling StatsdPackets*/

func TestStatsdPacketHandlerGauge(t *testing.T) {
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)
	sp := qtypes.NewStatsdPacket("gaugor", "333", "g")
	sd.HandlerStatsdPacket(sp)
	bkey := GenID("gaugor")
	assert.Equal(t, sd.Gauges[bkey], float64(333))

	// -10
	sp.ValFlt = 10
	sp.ValStr = "-"
	sd.HandlerStatsdPacket(sp)
	assert.Equal(t, sd.Gauges[bkey], float64(323))

	// +4
	sp.ValFlt = 4
	sp.ValStr = "+"
	sd.HandlerStatsdPacket(sp)
	assert.Equal(t, sd.Gauges[bkey], float64(327))

	// <0 overflow
	sp.ValFlt = 10
	sp.ValStr = ""
	sd.HandlerStatsdPacket(sp)
	sp.ValFlt = 20
	sp.ValStr = "-"
	sd.HandlerStatsdPacket(sp)
	assert.Equal(t, sd.Gauges[bkey], float64(0))

	// >MaxFloat64 overflow
	sp.ValFlt = float64(math.MaxFloat64 - 10)
	sp.ValStr = ""
	sd.HandlerStatsdPacket(sp)
	sp.ValFlt = 20
	sp.ValStr = "+"
	sd.HandlerStatsdPacket(sp)
	assert.Equal(t, sd.Gauges[bkey], float64(math.MaxFloat64))
}

func TestStatsdPacketHandlerGaugeWithDims(t *testing.T) {
	cfg := NewCfg()
	sd := NewStatsdaemon(cfg)
	sp := qtypes.NewStatsdPacketDims("gaugor", "333", "g", qtypes.NewDimensionsPre(map[string]string{"key1": "val1"}))
	sd.HandlerStatsdPacket(sp)
	bkey := GenID("gaugor_key1=val1")
	assert.Equal(t, sd.Gauges[bkey], float64(333))

	// -10
	sp.ValFlt = 10
	sp.ValStr = "-"
	sd.HandlerStatsdPacket(sp)
	assert.Equal(t, sd.Gauges[bkey], float64(323))

	// +4
	sp.ValFlt = 4
	sp.ValStr = "+"
	sd.HandlerStatsdPacket(sp)
	assert.Equal(t, sd.Gauges[bkey], float64(327))

	// <0 overflow
	sp.ValFlt = 10
	sp.ValStr = ""
	sd.HandlerStatsdPacket(sp)
	sp.ValFlt = 20
	sp.ValStr = "-"
	sd.HandlerStatsdPacket(sp)
	assert.Equal(t, sd.Gauges[bkey], float64(0))

	// >MaxFloat64 overflow
	sp.ValFlt = float64(math.MaxFloat64 - 10)
	sp.ValStr = ""
	sd.HandlerStatsdPacket(sp)
	sp.ValFlt = 20
	sp.ValStr = "+"
	sd.HandlerStatsdPacket(sp)
	assert.Equal(t, sd.Gauges[bkey], float64(math.MaxFloat64))
}

func BenchmarkOneBigTimer(t *testing.B) {
	pre := map[string]string{"statsd.percentiles": "99"}
	cfg := NewPreCfg(pre)
	sd := NewStatsdaemon(cfg)

	r := rand.New(rand.NewSource(438))
	bucket := "response_time"
	for i := 0; i < 10000000; i++ {
		a := float64(r.Uint32() % 1000)
		sd.Timers[bucket] = append(sd.Timers[bucket], a)
	}

	var buff bytes.Buffer
	t.ResetTimer()
	sd.ProcessTimers(&buff, time.Now().Unix())
}

func BenchmarkLotsOfTimers(t *testing.B) {
	pre := map[string]string{"statsd.percentiles": "99"}
	cfg := NewPreCfg(pre)
	sd := NewStatsdaemon(cfg)

	r := rand.New(rand.NewSource(438))
	for i := 0; i < 1000; i++ {
		bucket := "response_time" + strconv.Itoa(i)
		for i := 0; i < 10000; i++ {
			a := float64(r.Uint32() % 1000)
			sd.Timers[bucket] = append(sd.Timers[bucket], a)
		}
	}

	var buff bytes.Buffer
	t.ResetTimer()
	sd.ProcessTimers(&buff, time.Now().Unix())
}

func BenchmarkParseLineCounter(b *testing.B) {
	mp := NewMP()
	d1 := []byte("a.key.with-0.dash:4|c|@0.5")
	d2 := []byte("normal.key.space:1|c")

	for i := 0; i < b.N; i++ {
		mp.parseLine(d1)
		mp.parseLine(d2)
	}
}

func BenchmarkParseLineGauge(b *testing.B) {
	mp := NewMP()
	d1 := []byte("gaugor.whatever:333.4|g")
	d2 := []byte("gaugor.whatever:-5|g")

	for i := 0; i < b.N; i++ {
		mp.parseLine(d1)
		mp.parseLine(d2)
	}
}

func BenchmarkParseLineTimer(b *testing.B) {
	mp := NewMP()
	d1 := []byte("glork.some.keyspace:3.7211|ms")
	d2 := []byte("glork.some.keyspace:11223|ms")

	for i := 0; i < b.N; i++ {
		mp.parseLine(d1)
		mp.parseLine(d2)
	}
}

func BenchmarkParseLineSet(b *testing.B) {
	mp := NewMP()
	d1 := []byte("setof.some.keyspace:hiya|s")
	d2 := []byte("setof.some.keyspace:411|s")

	for i := 0; i < b.N; i++ {
		mp.parseLine(d1)
		mp.parseLine(d2)
	}
}
