package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/czcorpus/cnc-gokit/cors"
	"github.com/czcorpus/cnc-gokit/logging"
	"github.com/czcorpus/cnc-gokit/uniresp"
	"github.com/czcorpus/scollex/cnf"
	"github.com/czcorpus/scollex/engine"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
)

var (
	version   string
	buildDate string
	gitCommit string
)

// VersionInfo provides a detailed information about the actual build
type VersionInfo struct {
	Version   string `json:"version"`
	BuildDate string `json:"buildDate"`
	GitCommit string `json:"gitCommit"`
}

func init() {
}

func runApiServer(
	conf *cnf.Conf,
	syscallChan chan os.Signal,
	exitEvent chan os.Signal,
	sqlDB *pgx.Conn,
) {
	if !conf.LogLevel.IsDebugMode() {
		gin.SetMode(gin.ReleaseMode)
	}

	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(logging.GinMiddleware())
	engine.Use(uniresp.AlwaysJSONContentType())
	engine.Use(cors.CORSMiddleware(conf.CorsAllowedOrigins))
	engine.NoMethod(uniresp.NoMethodHandler)
	engine.NoRoute(uniresp.NotFoundHandler)

	fcollActions := NewActions(&conf.Corpora, sqlDB)

	engine.GET(
		"/fcoll/:corpusId/noun-modified-by", fcollActions.NounsModifiedBy)

	engine.GET(
		"/fcoll/:corpusId/modifiers-of", fcollActions.ModifiersOf)

	engine.GET(
		"/fcoll/:corpusId/verbs-subject", fcollActions.VerbsSubject)

	engine.GET(
		"/fcoll/:corpusId/verbs-object", fcollActions.VerbsObject)

	log.Info().Msgf("starting to listen at %s:%d", conf.ListenAddress, conf.ListenPort)
	srv := &http.Server{
		Handler:      engine,
		Addr:         fmt.Sprintf("%s:%d", conf.ListenAddress, conf.ListenPort),
		WriteTimeout: time.Duration(conf.ServerWriteTimeoutSecs) * time.Second,
		ReadTimeout:  time.Duration(conf.ServerReadTimeoutSecs) * time.Second,
	}
	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			log.Error().Err(err).Msg("")
		}
		syscallChan <- syscall.SIGTERM
	}()

	select {
	case <-exitEvent:
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := srv.Shutdown(ctx)
		if err != nil {
			log.Info().Err(err).Msg("Shutdown request error")
		}
	}
}

func main() {
	version := VersionInfo{
		Version:   version,
		BuildDate: buildDate,
		GitCommit: gitCommit,
	}

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "MQUERY - A specialized corpus querying server\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n\t%s [options] server [config.json]\n\t", filepath.Base(os.Args[0]))
		fmt.Fprintf(os.Stderr, "Usage:\n\t%s [options] worker [config.json]\n\t", filepath.Base(os.Args[0]))
		fmt.Fprintf(os.Stderr, "%s [options] version\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
	}
	flag.Parse()
	action := flag.Arg(0)
	if action == "version" {
		fmt.Printf("mquery %s\nbuild date: %s\nlast commit: %s\n", version.Version, version.BuildDate, version.GitCommit)
		return
	}
	conf := cnf.LoadConfig(flag.Arg(1))

	if action == "test" {
		cnf.ValidateAndDefaults(conf)
		log.Info().Msg("config OK")
		return

	} else {
		logging.SetupLogging(conf.LogFile, conf.LogLevel)
	}
	log.Info().Msg("Starting Scollex")
	cnf.ValidateAndDefaults(conf)
	syscallChan := make(chan os.Signal, 1)
	signal.Notify(syscallChan, os.Interrupt)
	signal.Notify(syscallChan, syscall.SIGTERM)
	exitEvent := make(chan os.Signal)

	go func() {
		evt := <-syscallChan
		exitEvent <- evt
		close(exitEvent)
	}()

	ctx := context.Background()
	pgDB, err := engine.OpenConnection(conf.DB, ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open database connection")
	}

	switch action {
	case "start":
		runApiServer(conf, syscallChan, exitEvent, pgDB)
	case "precalc":
		corpProps := conf.Corpora.GetCorpusProps(flag.Arg(2))
		if corpProps == nil {
			log.Fatal().Msgf("corpus %s not installed", flag.Arg(2))
			return
		}
		err := engine.RunPg(flag.Arg(2), flag.Arg(3), &corpProps.Syntax, pgDB)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to process")
		}
	default:
		log.Fatal().Msgf("Unknown action %s", action)
	}

}
