// Copyright 2023 Tomas Machalek <tomas.machalek@gmail.com>
// Copyright 2023 Institute of the Czech National Corpus,
//                Faculty of Arts, Charles University
//   This file is part of MQUERY.
//
//  MQUERY is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  MQUERY is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with MQUERY.  If not, see <https://www.gnu.org/licenses/>.

package main

import (
	"fmt"
	"math"
	"net/http"
	"sort"

	"github.com/czcorpus/cnc-gokit/uniresp"
	"github.com/czcorpus/scollex/cql"
	"github.com/czcorpus/scollex/engine"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
)

type Actions struct {
	corpora *engine.CorporaConf
	db      *pgx.Conn
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

	candidates, err := cdb.GetParentCandidates(w.V, w.PoS, "nmod", engine.CandidatesFreqLimit)
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
		}
		result[i] = item
	}
	sort.SliceStable(
		result,
		func(i, j int) bool {
			return result[j].CollWeight < result[i].CollWeight
		},
	)

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

	candidates, err := cdb.GetChildCandidates(w.V, w.PoS, "nmod", engine.CandidatesFreqLimit)
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
		}
		result[i] = item
	}
	sort.SliceStable(
		result,
		func(i, j int) bool {
			return result[j].CollWeight < result[i].CollWeight
		},
	)

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

	candidates, err := cdb.GetParentCandidates(w.V, w.PoS, "nsubj", engine.CandidatesFreqLimit)
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
		}
		result[i] = item
	}
	sort.SliceStable(
		result,
		func(i, j int) bool {
			return result[j].CollWeight < result[i].CollWeight
		},
	)

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

	candidates, err := cdb.GetParentCandidates(w.V, w.PoS, "obj|iobj", engine.CandidatesFreqLimit)
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
		}
		result[i] = item
	}
	sort.SliceStable(
		result,
		func(i, j int) bool {
			return result[j].CollWeight < result[i].CollWeight
		},
	)

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
	db *pgx.Conn,
) *Actions {
	return &Actions{
		corpora: corpora,
		db:      db,
	}
}
