package carbon

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/api"
	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/carbonserver"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/receiver"
	"github.com/go-graphite/go-carbon/tags"
	"github.com/lomik/zapwriter"

	// register receivers
	_ "github.com/go-graphite/go-carbon/receiver/http"
	_ "github.com/go-graphite/go-carbon/receiver/kafka"
	_ "github.com/go-graphite/go-carbon/receiver/pubsub"
	_ "github.com/go-graphite/go-carbon/receiver/tcp"
	_ "github.com/go-graphite/go-carbon/receiver/udp"
)

type NamedReceiver struct {
	receiver.Receiver
	Name string
}

type wspConfigRetriever struct {
	getRetentionFunc func(string) (int, bool)
	getAggrNameFunc  func(string) (string, float64, bool)
}

func (r *wspConfigRetriever) MetricRetentionPeriod(metric string) (int, bool) {
	return r.getRetentionFunc(metric)
}

func (r *wspConfigRetriever) MetricAggrConf(metric string) (string, float64, bool) {
	return r.getAggrNameFunc(metric)
}

type App struct {
	sync.RWMutex
	ConfigFilename string
	Config         *Config
	Api            *api.Api
	Cache          *cache.Cache
	Receivers      []*NamedReceiver
	CarbonLink     *cache.CarbonlinkListener
	Persister      *persister.Whisper
	Carbonserver   *carbonserver.CarbonserverListener
	Tags           *tags.Tags
	Collector      *Collector // (!!!) Should be re-created on every change config/modules
	PromRegisterer prometheus.Registerer
	PromRegistry   *prometheus.Registry
	exit           chan bool
	FlushTraces    func()
}

// New App instance
func New(configFilename string) *App {
	app := &App{
		ConfigFilename: configFilename,
		Config:         NewConfig(),
		PromRegistry:   prometheus.NewPedanticRegistry(),
		exit:           make(chan bool),
	}

	return app
}

// configure loads config from config file, schemas.conf, aggregation.conf
func (app *App) configure() error {
	var err error

	cfg, err := ReadConfig(app.ConfigFilename)
	if err != nil {
		return err
	}

	// carbon-cache prefix
	if hostname, err := os.Hostname(); err == nil {
		hostname = strings.ReplaceAll(hostname, ".", "_")
		cfg.Common.GraphPrefix = strings.ReplaceAll(cfg.Common.GraphPrefix, "{host}", hostname)
	} else {
		cfg.Common.GraphPrefix = strings.ReplaceAll(cfg.Common.GraphPrefix, "{host}", "localhost")
	}

	if cfg.Whisper.Enabled {
		cfg.Whisper.Schemas, err = persister.ReadWhisperSchemas(cfg.Whisper.SchemasFilename)
		if err != nil {
			return err
		}

		if cfg.Whisper.QuotasFilename != "" {
			cfg.Whisper.Quotas, err = persister.ReadWhisperQuotas(cfg.Whisper.QuotasFilename)
			if err != nil {
				return err
			}
		}

		if cfg.Whisper.AggregationFilename != "" {
			cfg.Whisper.Aggregation, err = persister.ReadWhisperAggregation(cfg.Whisper.AggregationFilename)
			if err != nil {
				return err
			}
		} else {
			cfg.Whisper.Aggregation = persister.NewWhisperAggregation()
		}
	}
	if !(cfg.Cache.WriteStrategy == "max" ||
		cfg.Cache.WriteStrategy == "sorted" ||
		cfg.Cache.WriteStrategy == "noop") {
		return fmt.Errorf("go-carbon support only \"max\", \"sorted\"  or \"noop\" write-strategy")
	}

	if cfg.Common.MetricEndpoint == "" {
		cfg.Common.MetricEndpoint = MetricEndpointLocal
	}

	if cfg.Common.MetricEndpoint != MetricEndpointLocal {
		u, err := url.Parse(cfg.Common.MetricEndpoint)

		if err != nil {
			return fmt.Errorf("common.metric-endpoint parse error: %s", err.Error())
		}

		if u.Scheme != "tcp" && u.Scheme != "udp" {
			return fmt.Errorf("common.metric-endpoint supports only tcp and udp protocols. %#v is unsupported", u.Scheme)
		}
	}

	app.Config = cfg

	return nil
}

// ParseConfig loads config from config file, schemas.conf, aggregation.conf
func (app *App) ParseConfig() error {
	app.Lock()
	defer app.Unlock()

	err := app.configure()

	if app.PromRegisterer == nil {
		app.PromRegisterer = prometheus.WrapRegistererWith(
			prometheus.Labels(app.Config.Prometheus.Labels),
			app.PromRegistry,
		)
	}

	return err
}

// ReloadConfig reloads some settings from config
func (app *App) ReloadConfig() error {
	app.Lock()
	defer app.Unlock()

	var err error
	if err = app.configure(); err != nil {
		return err
	}

	runtime.GOMAXPROCS(app.Config.Common.MaxCPU)

	app.Cache.SetMaxSize(app.Config.Cache.MaxSize)
	app.Cache.SetWriteStrategy(app.Config.Cache.WriteStrategy)
	app.Cache.SetTagsEnabled(app.Config.Tags.Enabled)
	app.Cache.SetBloomSize(app.Config.Cache.BloomSize)

	if app.Persister != nil {
		app.Persister.Stop()
		app.Persister = nil
	}

	if app.Tags != nil {
		app.Tags.Stop()
		app.Tags = nil
	}

	app.startPersister()

	if app.Collector != nil {
		app.Collector.Stop()
		app.Collector = nil
	}

	app.Collector = NewCollector(app)

	return nil
}

// Stop all socket listeners
// Assumes we are holding app.Lock()
func (app *App) stopListeners() {
	logger := zapwriter.Logger("app")

	if app.Api != nil {
		app.Api.Stop()
		app.Api = nil
		logger.Debug("api stopped")
	}

	if app.CarbonLink != nil {
		app.CarbonLink.Stop()
		app.CarbonLink = nil
		logger.Debug("carbonlink stopped")
	}

	if app.Carbonserver != nil {
		carbonserver := app.Carbonserver
		go func() {
			carbonserver.Stop()
			logger.Debug("carbonserver stopped")
		}()
		app.Carbonserver = nil
	}

	if app.Receivers != nil {
		for i := 0; i < len(app.Receivers); i++ {
			app.Receivers[i].Stop()
			logger.Debug("receiver stopped", zap.String("name", app.Receivers[i].Name))
		}
		app.Receivers = nil
	}

	if app.FlushTraces != nil {
		app.FlushTraces()
		logger.Debug("traces flushed")
	}
}

func (app *App) stopAll() {
	app.stopListeners()

	logger := zapwriter.Logger("app")

	if app.Persister != nil {
		app.Persister.Stop()
		app.Persister = nil
		logger.Debug("persister stopped")
	}

	if app.Tags != nil {
		app.Tags.Stop()
		app.Tags = nil
		logger.Debug("tags stopped")
	}

	if app.Cache != nil {
		app.Cache.Stop()
		app.Cache = nil
		logger.Debug("cache stopped")
	}

	if app.Collector != nil {
		app.Collector.Stop()
		app.Collector = nil
		logger.Debug("collector stopped")
	}

	if app.exit != nil {
		close(app.exit)
		app.exit = nil
		logger.Debug("close(exit)")
	}
}

// Stop force stop all components
func (app *App) Stop() {
	app.Lock()
	defer app.Unlock()
	app.stopAll()
}

func (app *App) startPersister() {
	if app.Config.Tags.Enabled {
		app.Tags = tags.New(&tags.Options{
			LocalPath:           app.Config.Tags.LocalDir,
			TagDB:               app.Config.Tags.TagDB,
			TagDBTimeout:        app.Config.Tags.TagDBTimeout.Value(),
			TagDBChunkSize:      app.Config.Tags.TagDBChunkSize,
			TagDBUpdateInterval: app.Config.Tags.TagDBUpdateInterval,
		})
	}

	if app.Config.Whisper.Enabled {
		p := persister.NewWhisper(
			app.Config.Whisper.DataDir,
			app.Config.Whisper.Schemas,
			app.Config.Whisper.Aggregation,
			app.Cache.WriteoutQueue().Get,
			app.Cache.PopNotConfirmed,
			app.Cache.Confirm,
			app.Cache.Pop,
		)
		p.SetMaxUpdatesPerSecond(app.Config.Whisper.MaxUpdatesPerSecond)
		p.SetSparse(app.Config.Whisper.Sparse)
		p.SetFLock(app.Config.Whisper.FLock)
		p.SetCompressed(app.Config.Whisper.Compressed)
		p.SetRemoveEmptyFile(app.Config.Whisper.RemoveEmptyFile)
		p.SetWorkers(app.Config.Whisper.Workers)
		p.SetHashFilenames(app.Config.Whisper.HashFilenames)
		if app.Config.Prometheus.Enabled {
			p.InitPrometheus(app.PromRegisterer)
		}
		if app.Tags != nil {
			p.SetTagsEnabled(true)
			p.SetTaggedFn(app.Tags.Add)
		}

		if cfg := app.Config.Whisper; cfg.OnlineMigration {
			scope := strings.Split(strings.TrimSpace(cfg.OnlineMigrationGlobalScope), ",")
			p.EnableOnlineMigration(cfg.OnlineMigrationRate, scope)
		}

		p.Start()
		app.Persister = p
	}
}

// Start starts
func (app *App) Start() (err error) {
	app.Lock()
	defer app.Unlock()

	defer func() {
		if err != nil {
			app.stopAll()
		}
	}()

	conf := app.Config

	runtime.GOMAXPROCS(conf.Common.MaxCPU)

	core := cache.New()
	core.SetMaxSize(conf.Cache.MaxSize)
	core.SetWriteStrategy(conf.Cache.WriteStrategy)
	core.SetTagsEnabled(conf.Tags.Enabled)
	core.SetBloomSize(conf.Cache.BloomSize)

	app.Cache = core

	/* API start */
	if conf.Grpc.Enabled {
		var grpcAddr *net.TCPAddr
		grpcAddr, err = net.ResolveTCPAddr("tcp", conf.Grpc.Listen)
		if err != nil {
			return
		}

		grpcApi := api.New(core)

		if err = grpcApi.Listen(grpcAddr); err != nil {
			return
		}

		app.Api = grpcApi
	}
	/* API end */

	/* WHISPER and TAGS start */
	app.startPersister()
	/* WHISPER and TAGS end */

	app.Receivers = make([]*NamedReceiver, 0)
	var rcv receiver.Receiver
	var rcvOptions map[string]interface{}

	/* UDP start */
	if conf.Udp.Enabled {
		if rcvOptions, err = receiver.WithProtocol(conf.Udp, "udp"); err != nil {
			return
		}

		if rcv, err = receiver.New("udp", rcvOptions, core.Add); err != nil {
			return
		}

		app.Receivers = append(app.Receivers, &NamedReceiver{
			Receiver: rcv,
			Name:     "udp",
		})
	}
	/* UDP end */

	/* TCP start */
	if conf.Tcp.Enabled {
		if rcvOptions, err = receiver.WithProtocol(conf.Tcp, "tcp"); err != nil {
			return
		}

		if rcv, err = receiver.New("tcp", rcvOptions, core.Add); err != nil {
			return
		}

		if conf.Prometheus.Enabled {
			rcv.InitPrometheus(app.PromRegisterer)
		}

		app.Receivers = append(app.Receivers, &NamedReceiver{
			Receiver: rcv,
			Name:     "tcp",
		})
	}
	/* TCP end */

	/* PICKLE start */
	if conf.Pickle.Enabled {
		if rcvOptions, err = receiver.WithProtocol(conf.Pickle, "pickle"); err != nil {
			return
		}

		if rcv, err = receiver.New("pickle", rcvOptions, core.Add); err != nil {
			return
		}

		app.Receivers = append(app.Receivers, &NamedReceiver{
			Receiver: rcv,
			Name:     "pickle",
		})
	}
	/* PICKLE end */

	/* CUSTOM RECEIVERS start */
	for receiverName, receiverOptions := range conf.Receiver {
		if rcv, err = receiver.New(receiverName, receiverOptions, core.Add); err != nil {
			return
		}

		app.Receivers = append(app.Receivers, &NamedReceiver{
			Receiver: rcv,
			Name:     receiverName,
		})
	}
	/* CUSTOM RECEIVERS end */

	/* CARBONSERVER start */
	if conf.Carbonserver.Enabled {
		if err != nil {
			return
		}

		if conf.Carbonserver.TrigramIndex || conf.Carbonserver.TrieIndex {
			if fi, err := os.Lstat(conf.Whisper.DataDir); err != nil {
				return fmt.Errorf("failed to stat whisper data directory: %s", err)
			} else if fi.Mode()&os.ModeSymlink == 1 {
				return fmt.Errorf("whisper data directory is a symlink")
			}
		}

		apiPerPathRateLimiters := map[string]*carbonserver.ApiPerPathRatelimiter{}
		for _, rl := range conf.Carbonserver.APIPerPathRateLimiters {
			var timeout time.Duration
			if rl.RequestTimeout != nil {
				timeout = rl.RequestTimeout.Value()
			}
			apiPerPathRateLimiters[rl.Path] = carbonserver.NewApiPerPathRatelimiter(rl.MaxInflightRequests, timeout)
		}
		var globQueryRateLimiters []*carbonserver.GlobQueryRateLimiter
		for _, rl := range conf.Carbonserver.HeavyGlobQueryRateLimiters {
			gqrl, err := carbonserver.NewGlobQueryRateLimiter(rl.Pattern, rl.MaxInflightRequests)
			if err != nil {
				return fmt.Errorf("failed to init Carbonserver.HeavyGlobQueryRateLimiters %s: %s", rl.Pattern, err)
			}
			globQueryRateLimiters = append(globQueryRateLimiters, gqrl)
		}

		// TODO: refactor: do not use var name the same as pkg name
		carbonserver := carbonserver.NewCarbonserverListener(core.Get)
		carbonserver.SetWhisperData(conf.Whisper.DataDir)
		carbonserver.SetMaxGlobs(conf.Carbonserver.MaxGlobs)
		carbonserver.SetEmptyResultOk(conf.Carbonserver.EmptyResultOk)
		carbonserver.SetDoNotLog404s(conf.Carbonserver.DoNotLog404s)
		carbonserver.SetFLock(app.Config.Whisper.FLock)
		carbonserver.SetCompressed(app.Config.Whisper.Compressed)
		carbonserver.SetRemoveEmptyFile(app.Config.Whisper.RemoveEmptyFile)
		carbonserver.SetFailOnMaxGlobs(conf.Carbonserver.FailOnMaxGlobs)
		carbonserver.SetMaxMetricsGlobbed(conf.Carbonserver.MaxMetricsGlobbed)
		carbonserver.SetMaxMetricsRendered(conf.Carbonserver.MaxMetricsRendered)
		carbonserver.SetBuckets(conf.Carbonserver.Buckets)
		carbonserver.SetMetricsAsCounters(conf.Carbonserver.MetricsAsCounters)
		carbonserver.SetScanFrequency(conf.Carbonserver.ScanFrequency.Value())
		carbonserver.SetQuotaUsageReportFrequency(conf.Carbonserver.QuotaUsageReportFrequency.Value())
		carbonserver.SetMaxCreatesPerSecond(conf.Carbonserver.MaxCreatesPerSecond)
		carbonserver.SetReadTimeout(conf.Carbonserver.ReadTimeout.Value())
		carbonserver.SetIdleTimeout(conf.Carbonserver.IdleTimeout.Value())
		carbonserver.SetWriteTimeout(conf.Carbonserver.WriteTimeout.Value())
		carbonserver.SetQueryCacheEnabled(conf.Carbonserver.QueryCacheEnabled)
		carbonserver.SetStreamingQueryCacheEnabled(conf.Carbonserver.StreamingQueryCacheEnabled)
		carbonserver.SetFindCacheEnabled(conf.Carbonserver.FindCacheEnabled)
		carbonserver.SetQueryCacheSizeMB(conf.Carbonserver.QueryCacheSizeMB)
		carbonserver.SetTrigramIndex(conf.Carbonserver.TrigramIndex)
		carbonserver.SetTrieIndex(conf.Carbonserver.TrieIndex)
		carbonserver.SetConcurrentIndex(conf.Carbonserver.ConcurrentIndex)
		carbonserver.SetFileListCache(conf.Carbonserver.FileListCache)
		carbonserver.SetFileListCacheVersion(conf.Carbonserver.FileListCacheVersion)
		carbonserver.SetMultiThreadWalkEnabled(conf.Carbonserver.MultiThreadWalkEnabled)
		carbonserver.SetInternalStatsDir(conf.Carbonserver.InternalStatsDir)
		carbonserver.SetPercentiles(conf.Carbonserver.Percentiles)
		// carbonserver.SetQueryTimeout(conf.Carbonserver.QueryTimeout.Value())

		carbonserver.SetMaxInflightRequests(conf.Carbonserver.MaxInflightRequests)
		carbonserver.SetNoServiceWhenIndexIsNotReady(conf.Carbonserver.NoServiceWhenIndexIsNotReady)
		carbonserver.SetRenderTraceLoggingEnabled(conf.Carbonserver.RenderTraceLoggingEnabled)

		if conf.Carbonserver.RequestTimeout != nil {
			carbonserver.SetRequestTimeout(conf.Carbonserver.RequestTimeout.Value())
		}
		if len(apiPerPathRateLimiters) > 0 {
			carbonserver.SetAPIPerPathRateLimiter(apiPerPathRateLimiters)
		}
		if len(globQueryRateLimiters) > 0 {
			carbonserver.SetHeavyGlobQueryRateLimiters(globQueryRateLimiters)
		}

		if app.Config.Whisper.Quotas != nil {
			if !conf.Carbonserver.ConcurrentIndex || conf.Carbonserver.RealtimeIndex <= 0 {
				return errors.New("concurrent-index and realtime-index needs to be enabled for quota control.")
			}

			carbonserver.SetEstimateSize(func(metric string) (logicalSize, physicalSize, dataPoints int64) {
				schema, ok := app.Config.Whisper.Schemas.Match(metric)

				if !ok {
					// Why not configurable: go-carbon users
					// should always make sure that there is a default retention policy.
					return 4096 + 172800*12, 4096 + 172800*12, 172800 // 2 days of secondly data
				}

				for _, r := range schema.Retentions {
					dataPoints += int64(r.NumberOfPoints())
				}
				logicalSize = 4096 + dataPoints*12
				if app.Config.Whisper.Sparse { // we assume that physical size for sparse metrics takes only a part of the logical size
					physicalSize = int64(app.Config.Whisper.PhysicalSizeFactor * float32(logicalSize))
				} else {
					physicalSize = logicalSize
				}

				return logicalSize, physicalSize, dataPoints
			})

			carbonserver.SetQuotas(app.Config.getCarbonserverQuotas())
			core.SetThrottle(carbonserver.ShouldThrottleMetric)
		}

		var setConfigRetriever bool
		if conf.Carbonserver.CacheScan {
			core.InitCacheScanAdds()
			carbonserver.SetCacheGetMetricsFunc(core.GetRecentNewMetrics)

			setConfigRetriever = true
		}

		if conf.Carbonserver.RealtimeIndex > 0 {
			ch := carbonserver.SetRealtimeIndex(conf.Carbonserver.RealtimeIndex)
			core.SetNewMetricsChan(ch)

			setConfigRetriever = true
		}
		if setConfigRetriever {
			retriever := &wspConfigRetriever{
				getRetentionFunc: app.Persister.GetRetentionPeriod,
				getAggrNameFunc:  app.Persister.GetAggrConf,
			}
			carbonserver.SetConfigRetriever(retriever)
		}

		if conf.Prometheus.Enabled {
			carbonserver.InitPrometheus(app.PromRegisterer)
		}
		if conf.Tracing.Enabled {
			log.Printf("Otel tracing is removed in current verion, ignoring tracing.enabled=true")
		}

		carbonserver.RegisterInternalInfoHandler("cache", core.GetInfo)
		carbonserver.RegisterInternalInfoHandler("config", func() map[string]interface{} {
			infos := map[string]interface{}{}
			for name, file := range map[string]string{
				"app":         app.ConfigFilename,
				"schema":      app.Config.Whisper.SchemasFilename,
				"aggregation": app.Config.Whisper.AggregationFilename,
				"quota":       app.Config.Whisper.QuotasFilename,
			} {
				if data, err := os.ReadFile(file); err != nil {
					infos[name] = err.Error()
				} else {
					infos[name] = string(data)
				}
			}
			return infos
		})

		if err = carbonserver.Listen(conf.Carbonserver.Listen); err != nil {
			return
		}

		if conf.Carbonserver.Grpc.Enabled {
			if err = carbonserver.ListenGRPC(conf.Carbonserver.Grpc.Listen); err != nil {
				return
			}
		}

		app.Carbonserver = carbonserver
	}
	/* CARBONSERVER end */

	/* CARBONLINK start */
	if conf.Carbonlink.Enabled {
		var linkAddr *net.TCPAddr
		linkAddr, err = net.ResolveTCPAddr("tcp", conf.Carbonlink.Listen)
		if err != nil {
			return
		}

		carbonlink := cache.NewCarbonlinkListener(core)
		carbonlink.SetReadTimeout(conf.Carbonlink.ReadTimeout.Value())
		// carbonlink.SetQueryTimeout(conf.Carbonlink.QueryTimeout.Value())

		if err = carbonlink.Listen(linkAddr); err != nil {
			return
		}

		app.CarbonLink = carbonlink
	}
	/* CARBONLINK end */

	/* RESTORE start */
	if conf.Dump.Enabled {
		go app.Restore(core.Add, conf.Dump.Path, conf.Dump.RestorePerSecond)
	}
	/* RESTORE end */

	/* COLLECTOR start */
	app.Collector = NewCollector(app)
	/* COLLECTOR end */

	return
}

// Loop ...
func (app *App) Loop() {
	app.RLock()
	exitChan := app.exit
	app.RUnlock()

	if exitChan != nil {
		<-app.exit
	}
}

func (app *App) CheckPersisterPolicyConsistencies(rate int, printInconsistentMetrics bool) {
	p := persister.NewWhisper(
		app.Config.Whisper.DataDir,
		app.Config.Whisper.Schemas,
		app.Config.Whisper.Aggregation,
		nil, nil, nil, nil,
	)
	err := p.CheckPolicyConsistencies(rate, printInconsistentMetrics)
	if err != nil {
		log.Printf("failed to check policy consistencies: %s\n", err)
		return
	}
}
