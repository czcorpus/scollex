// Copyright 2023 Tomas Machalek <tomas.machalek@gmail.com>
// Copyright 2023 Institute of the Czech National Corpus,
// Faculty of Arts, Charles University
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"database/sql"
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
	sqlDB *sql.DB,
) {
	if !conf.Logging.Level.IsDebugMode() {
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
		"/query/:corpusId/noun-modified-by", fcollActions.NounsModifiedBy)

	engine.GET(
		"/query/:corpusId/modifiers-of", fcollActions.ModifiersOf)

	engine.GET(
		"/query/:corpusId/verbs-subject", fcollActions.VerbsSubject)

	engine.GET(
		"/query/:corpusId/verbs-object", fcollActions.VerbsObject)

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

	<-exitEvent
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := srv.Shutdown(ctx)
	if err != nil {
		log.Info().Err(err).Msg("Shutdown request error")
	}

}

func main() {
	version := VersionInfo{
		Version:   version,
		BuildDate: buildDate,
		GitCommit: gitCommit,
	}

	generalUsage := func() {
		fmt.Fprintf(os.Stderr, "SCollEx - a Syntactic Collocations explorer\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\t%s [options] start [config.json]\n", filepath.Base(os.Args[0]))
		fmt.Fprintf(os.Stderr, "\t%s [options] import [config.json] [corpus ID] [path to vertical file]\n", filepath.Base(os.Args[0]))
		fmt.Fprintf(os.Stderr, "\t%s [options] test [config.json]\n", filepath.Base(os.Args[0]))
		fmt.Fprintf(os.Stderr, "\t%s [options] version\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
	}

	startCmd := flag.NewFlagSet("start", flag.ExitOnError)
	startCmd.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage:\t%s [options] start [config.json]\n", filepath.Base(os.Args[0]))
		startCmd.PrintDefaults()
	}
	importCmd := flag.NewFlagSet("import", flag.ExitOnError)
	forceOverwriteTbl := importCmd.Bool("f", false, "Drop target tables in case they already exist")
	coOccSpan := importCmd.Int("colloc-flags-with-span", 2, "Defines window size for calculating coocurrences")

	action := os.Args[1]
	if action == "version" {
		fmt.Printf("scollex %s\nbuild date: %s\nlast commit: %s\n", version.Version, version.BuildDate, version.GitCommit)
		return
	}

	switch action {
	case "start":
		startCmd.Parse(os.Args[2:])
		conf := cnf.LoadConfig(startCmd.Arg(0))

		if action == "test" {
			cnf.ValidateAndDefaults(conf)
			log.Info().Msg("config OK")
			return

		} else {
			logging.SetupLogging(conf.Logging)
		}
		log.Info().Msg("Starting SCollEx")
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

		sqlDB, err := engine.Open(conf.DB)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to open database connection")
		}

		runApiServer(conf, syscallChan, exitEvent, sqlDB)
	case "import":
		importCmd.Parse(os.Args[2:])
		conf := cnf.LoadConfig(importCmd.Arg(0))
		sqlDB, err := engine.Open(conf.DB)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to open database connection")
		}

		corpProps := conf.Corpora.GetCorpusProps(importCmd.Arg(1))
		if corpProps == nil {
			log.Fatal().Msgf("corpus `%s` not installed", importCmd.Arg(1))
			return
		}
		cdb := engine.NewCollDatabase(sqlDB, importCmd.Arg(1))
		err = cdb.InitializeDB(sqlDB, *forceOverwriteTbl)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to initialize database tables")
		}
		log.Info().Msgf("Testing whether the table %s is ready", cdb.TableName())
		err = cdb.TestTableReady()
		if err != nil {
			log.Fatal().
				Err(err).
				Str("dbHost", conf.DB.Host).
				Int("dbPort", conf.DB.Port).
				Str("dbName", conf.DB.Name).
				Msg("...target db table NOT READY")
			return

		} else {
			log.Info().Msg("... table READY")
		}
		err = engine.RunPg(importCmd.Arg(1), importCmd.Arg(2), *coOccSpan, &corpProps.Syntax, sqlDB)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to process")
			return
		}
	default:
		generalUsage()
	}

}
