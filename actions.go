// Copyright 2023 Tomas Machalek <tomas.machalek@gmail.com>
// Copyright 2023 Institute of the Czech National Corpus,
//                Faculty of Arts, Charles University
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
	"database/sql"
	"fmt"
	"math"
	"net/http"
	"sort"

	"github.com/czcorpus/cnc-gokit/unireq"
	"github.com/czcorpus/cnc-gokit/uniresp"
	"github.com/czcorpus/scollex/cql"
	"github.com/czcorpus/scollex/engine"
	"github.com/gin-gonic/gin"
)

type Actions struct {
	corpora *engine.CorporaConf
	db      *sql.DB
}

func (a *Actions) NounsModifiedBy(ctx *gin.Context) {
	w := engine.Word{V: ctx.Request.URL.Query().Get("w"), PoS: ctx.Request.URL.Query().Get("pos")}
	if !w.IsValid() {
		uniresp.RespondWithErrorJSON(
			ctx,
			uniresp.NewActionError("invalid word value"),
			http.StatusUnprocessableEntity,
		)
		return
	}
	maxItems, ok := unireq.GetURLIntArgOrFail(ctx, "maxItems", 10)
	if !ok {
		return
	}
	corpusID := ctx.Param("corpusId")
	corpusConf := a.corpora.GetCorpusProps(corpusID)
	if corpusConf == nil {
		uniresp.RespondWithErrorJSON(ctx, fmt.Errorf("corpus not found"), http.StatusInternalServerError)
		return
	}
	// [lemma="team" & deprel="nmod" & p_upos="NOUN"]
	cdb := engine.NewCollDatabase(a.db, corpusID)

	fx, err := cdb.GetFreq(w.V, w.PoS, "", "NOUN", "nmod")
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}

	candidates, err := cdb.GetCollCandidatesOfChild(w.V, w.PoS, "nmod", engine.CandidatesFreqLimit)
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}

	result := make(engine.FreqDistribItemList, len(candidates))
	for i, cand := range candidates {

		item := &engine.FreqDistribItem{
			Word:       cand.Lemma,
			Freq:       cand.FreqXY,
			IPM:        float32(cand.FreqXY) / float32(corpusConf.Size) * 1e6,
			CollWeight: 14 + math.Log2(2*float64(cand.FreqXY)/(float64(fx)+float64(cand.FreqY))),
			CoOccScore: cand.CoOccScore,
		}
		result[i] = item
	}
	sort.SliceStable(
		result,
		func(i, j int) bool {
			return result[j].CollWeight < result[i].CollWeight
		},
	)
	result = result.Cut(maxItems)
	resp := engine.FreqDistrib{
		Freqs:            result,
		CorpusSize:       corpusConf.Size,
		ExamplesQueryTpl: cql.NounsModifiedBy(&corpusConf.Syntax, w, "%s"),
	}
	uniresp.WriteJSONResponse(
		ctx.Writer,
		resp,
	)
}

func (a *Actions) ModifiersOf(ctx *gin.Context) {
	w := engine.Word{V: ctx.Request.URL.Query().Get("w"), PoS: ctx.Request.URL.Query().Get("pos")}
	if !w.IsValid() {
		uniresp.RespondWithErrorJSON(
			ctx,
			uniresp.NewActionError("invalid word value"),
			http.StatusUnprocessableEntity,
		)
		return
	}
	maxItems, ok := unireq.GetURLIntArgOrFail(ctx, "maxItems", 10)
	if !ok {
		return
	}
	corpusID := ctx.Param("corpusId")
	corpusConf := a.corpora.GetCorpusProps(corpusID)
	if corpusConf == nil {
		uniresp.RespondWithErrorJSON(ctx, fmt.Errorf("corpus not found"), http.StatusInternalServerError)
		return
	}
	// [p_lemma="team" & deprel="nmod" & upos="NOUN"]
	cdb := engine.NewCollDatabase(a.db, corpusID)

	fx, err := cdb.GetFreq("", "NOUN", w.V, w.PoS, "nmod")

	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}

	candidates, err := cdb.GetCollCandidatesOfParent(w.V, w.PoS, "nmod", engine.CandidatesFreqLimit)
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}

	result := make(engine.FreqDistribItemList, len(candidates))
	for i, cand := range candidates {

		item := &engine.FreqDistribItem{
			Word:       cand.Lemma,
			Freq:       cand.FreqXY,
			IPM:        float32(cand.FreqXY) / float32(corpusConf.Size) * 1e6,
			CollWeight: 14 + math.Log2(2*float64(cand.FreqXY)/(float64(fx)+float64(cand.FreqY))),
			CoOccScore: cand.CoOccScore,
		}
		result[i] = item
	}
	sort.SliceStable(
		result,
		func(i, j int) bool {
			return result[j].CollWeight < result[i].CollWeight
		},
	)
	result = result.Cut(maxItems)
	resp := engine.FreqDistrib{
		Freqs:            result,
		CorpusSize:       corpusConf.Size,
		ExamplesQueryTpl: cql.ModifiersOf(&corpusConf.Syntax, w, "%s"),
	}
	uniresp.WriteJSONResponse(
		ctx.Writer,
		resp,
	)
}

// VerbsSubject
func (a *Actions) VerbsSubject(ctx *gin.Context) {
	w := engine.Word{V: ctx.Request.URL.Query().Get("w"), PoS: ctx.Request.URL.Query().Get("pos")}
	if !w.IsValid() {
		uniresp.RespondWithErrorJSON(
			ctx,
			uniresp.NewActionError("invalid word value"),
			http.StatusUnprocessableEntity,
		)
		return
	}
	maxItems, ok := unireq.GetURLIntArgOrFail(ctx, "maxItems", 10)
	if !ok {
		return
	}
	corpusID := ctx.Param("corpusId")
	corpusConf := a.corpora.GetCorpusProps(corpusID)
	if corpusConf == nil {
		uniresp.RespondWithErrorJSON(ctx, fmt.Errorf("corpus not found"), http.StatusInternalServerError)
		return
	}
	// [lemma="team" & deprel="nsubj" & p_upos="VERB"]
	cdb := engine.NewCollDatabase(a.db, corpusID)

	fx, err := cdb.GetFreq(w.V, w.PoS, "", "VERB", "nsubj")
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}

	candidates, err := cdb.GetCollCandidatesOfChild(w.V, w.PoS, "nsubj", engine.CandidatesFreqLimit)
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}

	result := make(engine.FreqDistribItemList, len(candidates))
	for i, cand := range candidates {

		item := &engine.FreqDistribItem{
			Word:       cand.Lemma,
			Freq:       cand.FreqXY,
			IPM:        float32(cand.FreqXY) / float32(corpusConf.Size) * 1e6,
			CollWeight: 14 + math.Log2(2*float64(cand.FreqXY)/(float64(fx)+float64(cand.FreqY))),
			CoOccScore: cand.CoOccScore,
		}
		result[i] = item
	}
	sort.SliceStable(
		result,
		func(i, j int) bool {
			return result[j].CollWeight < result[i].CollWeight
		},
	)
	result = result.Cut(maxItems)
	resp := engine.FreqDistrib{
		Freqs:            result,
		CorpusSize:       corpusConf.Size,
		ExamplesQueryTpl: cql.VerbsSubject(&corpusConf.Syntax, w, "%s"),
	}
	uniresp.WriteJSONResponse(
		ctx.Writer,
		resp,
	)
}

// VerbsObject
func (a *Actions) VerbsObject(ctx *gin.Context) {
	w := engine.Word{V: ctx.Request.URL.Query().Get("w"), PoS: ctx.Request.URL.Query().Get("pos")}
	if !w.IsValid() {
		uniresp.RespondWithErrorJSON(
			ctx,
			uniresp.NewActionError("invalid word value"),
			http.StatusUnprocessableEntity,
		)
		return
	}
	maxItems, ok := unireq.GetURLIntArgOrFail(ctx, "maxItems", 10)
	if !ok {
		return
	}
	corpusID := ctx.Param("corpusId")
	corpusConf := a.corpora.GetCorpusProps(corpusID)
	if corpusConf == nil {
		uniresp.RespondWithErrorJSON(ctx, fmt.Errorf("corpus not found"), http.StatusInternalServerError)
		return
	}
	// [lemma="team" & deprel="obj|iobj" & p_upos="VERB"]
	cdb := engine.NewCollDatabase(a.db, corpusID)

	fx, err := cdb.GetFreq(w.V, w.PoS, "", "VERB", "obj|iobj")
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}

	candidates, err := cdb.GetCollCandidatesOfChild(w.V, w.PoS, "obj|iobj", engine.CandidatesFreqLimit)
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}

	result := make(engine.FreqDistribItemList, len(candidates))
	for i, cand := range candidates {

		item := &engine.FreqDistribItem{
			Word:       cand.Lemma,
			Freq:       cand.FreqXY,
			IPM:        float32(cand.FreqXY) / float32(corpusConf.Size) * 1e6,
			CollWeight: 14 + math.Log2(2*float64(cand.FreqXY)/(float64(fx)+float64(cand.FreqY))),
			CoOccScore: cand.CoOccScore,
		}
		result[i] = item
	}
	sort.SliceStable(
		result,
		func(i, j int) bool {
			return result[j].CollWeight < result[i].CollWeight
		},
	)
	result = result.Cut(maxItems)
	resp := engine.FreqDistrib{
		Freqs:            result,
		CorpusSize:       corpusConf.Size,
		ExamplesQueryTpl: cql.VerbsObject(&corpusConf.Syntax, w, "%s"),
	}
	uniresp.WriteJSONResponse(
		ctx.Writer,
		resp,
	)
}

func NewActions(
	corpora *engine.CorporaConf,
	db *sql.DB,
) *Actions {
	return &Actions{
		corpora: corpora,
		db:      db,
	}
}
