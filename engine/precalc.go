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

package engine

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/czcorpus/cnc-gokit/collections"
	"github.com/rs/zerolog/log"
	"github.com/tomachalek/vertigo/v5"
)

const (
	bulkInsertChunkSize = 1000
)

type FyItem struct {
	Lemma  string
	Upos   string
	Deprel string
	Freq   int64
}

type FyTable map[string]*FyItem

func (table FyTable) mkKey(lemma, upos, deprel string) string {
	return fmt.Sprintf("%s:%s:%s", lemma, upos, deprel)
}

func (table FyTable) Add(lemma, upos, deprel string, val int64) {
	key := table.mkKey(lemma, upos, deprel)
	v, ok := table[key]
	if !ok {
		v = &FyItem{
			Lemma:  lemma,
			Upos:   upos,
			Deprel: deprel,
		}
		table[key] = v
	}
	v.Freq += val
}

func (table FyTable) Has(lemma, upos, deprel string) bool {
	key := table.mkKey(lemma, upos, deprel)
	_, ok := table[key]
	return ok
}

type CTItem struct {
	Lemma  string
	PLemma string
	Deprel string
	Upos   string
	PUpos  string
	Freq   int64
}

type CounterTable map[string]*CTItem

func (table CounterTable) mkKey(lemma, upos, pLemma, pUpos, deprel string) string {
	return fmt.Sprintf("%s:%s:%s:%s:%s", lemma, upos, deprel, pLemma, pUpos)
}

func (table CounterTable) Add(lemma, upos, pLemma, pUpos, deprel string, val int64) {
	key := table.mkKey(lemma, upos, pLemma, pUpos, deprel)
	v, ok := table[key]
	if !ok {
		v = &CTItem{
			Lemma:  lemma,
			Upos:   upos,
			PLemma: pLemma,
			PUpos:  pUpos,
			Deprel: deprel,
		}
		table[key] = v
	}
	v.Freq += val
}

type CoTItem struct {
	Lemma   string
	CoLemma string
	Upos    string
	CoUpos  string
	Freq    int64
}

type CoOccTable map[string]*CoTItem

func (table CoOccTable) mkKey(lemma, upos, coLemma, coUpos string) string {
	return fmt.Sprintf("%s:%s::%s:%s", lemma, upos, coLemma, coUpos)
}

func (table CoOccTable) Add(lemma, upos, coLemma, coUpos string, val int64) {
	key := table.mkKey(lemma, upos, coLemma, coUpos)
	v, ok := table[key]
	if !ok {
		v = &CoTItem{
			Lemma:   lemma,
			Upos:    upos,
			CoLemma: coLemma,
			CoUpos:  coUpos,
		}
		table[key] = v
	}
	v.Freq += val
}

func (table CoOccTable) Has(lemma, upos, coLemma, coUpos string) bool {
	key := table.mkKey(lemma, upos, coLemma, coUpos)
	_, ok := table[key]
	return ok
}

func expandDeprelMultivalue(value string) []string {
	ans := make([]string, 0, 2)
	tmp := strings.Split(value, "|")
	if len(tmp) > 2 {
		log.Warn().
			Str("expression", value).
			Msg("deprel expression not fully supported")
		ans = append(ans, tmp...)
	}
	// this along with individual items does not cover whole
	// expression but it should be ok
	ans = append(ans, value)
	return ans
}

func expandDeprelMultivalues(values []string) []string {
	ans := make([]string, 0, len(values)+2)
	for _, v := range values {
		ans = append(ans, expandDeprelMultivalue(v)...)
	}
	return ans
}

type CoVertProcessor struct {
	Span        int
	Window      [][2]string
	conf        *SyntaxProps
	CoOccTable  CoOccTable
	TokenCounts FyTable
}

func (cvp *CoVertProcessor) ProcToken(token *vertigo.Token, line int, err error) error {
	if err != nil {
		return err
	}
	if len(token.Attrs) < 12 {
		log.Error().Msgf("Too few token columns on line %d", line)
		return nil
	}
	lemma := token.Attrs[cvp.conf.LemmaAttr.VerticalCol-1]
	upos := token.Attrs[cvp.conf.PosAttr.VerticalCol-1]
	if cvp.TokenCounts.Has(lemma, upos, "") {
		cvp.TokenCounts.Add(lemma, upos, "", 1)
	}

	if len(cvp.Window) == 2*cvp.Span+1 {
		cvp.Window = append(cvp.Window[1:], [2]string{lemma, upos})
	} else {
		cvp.Window = append(cvp.Window, [2]string{lemma, upos})
	}

	if len(cvp.Window) == 2*cvp.Span+1 {
		middle := cvp.Window[cvp.Span]
		for i, near := range cvp.Window {
			if i != cvp.Span && cvp.CoOccTable.Has(middle[0], middle[1], near[0], near[1]) {
				cvp.CoOccTable.Add(middle[0], middle[1], near[0], near[1], 1)
			}
		}
	}
	return nil
}

func (cvp *CoVertProcessor) ProcStruct(strc *vertigo.Structure, line int, err error) error {
	return nil
}

func (cvp *CoVertProcessor) ProcStructClose(strc *vertigo.StructureClose, line int, err error) error {
	return nil
}

type VertProcessor struct {
	DeprelCol    int
	DeprelTypes  []string
	conf         *SyntaxProps
	Table        CounterTable
	ParentCounts FyTable
	ChildCounts  FyTable
}

func (vp *VertProcessor) ProcToken(token *vertigo.Token, line int, err error) error {
	if err != nil {
		return err
	}
	if len(token.Attrs) < 12 {
		log.Error().Msgf("Too few token columns on line %d", line)
		return nil
	}
	// below, we index always [k-1] because `word` in Vertigo is separated
	deprelTmp := token.Attrs[vp.conf.FuncAttr.VerticalCol-1]
	lemma := token.Attrs[vp.conf.LemmaAttr.VerticalCol-1]
	upos := token.Attrs[vp.conf.PosAttr.VerticalCol-1]
	pLemma := token.Attrs[vp.conf.ParLemmaAttr.VerticalCol-1]
	pUpos := token.Attrs[vp.conf.ParPosAttr.VerticalCol-1]
	for _, deprel := range expandDeprelMultivalue(deprelTmp) {
		if collections.SliceContains(vp.DeprelTypes, deprel) {
			vp.Table.Add(lemma, upos, pLemma, pUpos, deprel, 1)
			vp.ParentCounts.Add(pLemma, pUpos, deprel, 1)
			vp.ChildCounts.Add(lemma, upos, deprel, 1)
		}
	}
	//useFirstNonWordPosAttr(tokenAttrs[0])

	return nil
}

func (vp *VertProcessor) ProcStruct(strc *vertigo.Structure, line int, err error) error {
	return nil
}

func (vp *VertProcessor) ProcStructClose(strc *vertigo.StructureClose, line int, err error) error {
	return nil
}

func writeFxy(tx *sql.Tx, table CounterTable, coOccTable CoOccTable, tokenCounts FyTable, corpusID string) error {
	var i int
	args := make([]any, 0, bulkInsertChunkSize*7)
	insertPlaceholders := make([]string, 0, bulkInsertChunkSize)

	for _, v := range table {
		if i == bulkInsertChunkSize {
			sql := fmt.Sprintf(
				"INSERT INTO %s_fcolls (lemma, upos, p_lemma, p_upos, deprel, freq, co_occurrence_score) VALUES %s",
				corpusID, strings.Join(insertPlaceholders, ", "))
			_, err := tx.Exec(sql, args...)
			if err != nil {
				tx.Rollback()
				return err
			}
			args = make([]any, 0, bulkInsertChunkSize*7)
			insertPlaceholders = make([]string, 0, bulkInsertChunkSize)
			i = 0
			log.Debug().Int("items", bulkInsertChunkSize).Msg("written Fxy bulk into database")
		}

		fxy := coOccTable[coOccTable.mkKey(v.Lemma, v.Upos, v.PLemma, v.PUpos)]
		fx := tokenCounts[tokenCounts.mkKey(v.Lemma, v.Upos, "")]
		fy := tokenCounts[tokenCounts.mkKey(v.PLemma, v.PUpos, "")]
		logDice := 14 + math.Log2(2*float64(fxy.Freq)/float64(fx.Freq+fy.Freq))

		// Replace SQL invalid float values
		if math.IsInf(logDice, 1) {
			logDice = 3.4e38 // Substitute Inf with max float
		} else if math.IsInf(logDice, -1) {
			logDice = -3.4e38 // Substitute -Inf with min float
		} else if math.IsNaN(logDice) {
			logDice = 0 // Substitute NaN with 0
		}

		args = append(args, v.Lemma, v.Upos, v.PLemma, v.PUpos, v.Deprel, v.Freq, logDice)
		insertPlaceholders = append(insertPlaceholders, "(?, ?, ?, ?, ?, ?, ?)")
		i++
	}

	if len(args) > 0 {
		sql := fmt.Sprintf(
			"INSERT INTO %s_fcolls (lemma, upos, p_lemma, p_upos, deprel, freq, co_occurrence_score) VALUES %s",
			corpusID, strings.Join(insertPlaceholders, ", "))
		_, err := tx.Exec(sql, args...)
		if err != nil {
			tx.Rollback()
			return err
		}
		log.Debug().Int("items", len(insertPlaceholders)).Msg("written Fxy bulk into database")
	}
	return nil
}

func writeParents(tx *sql.Tx, table FyTable, corpusID string) error {
	var i int
	args := make([]any, 0, bulkInsertChunkSize*4)
	insertPlaceholders := make([]string, 0, bulkInsertChunkSize)

	for _, v := range table {
		if i == bulkInsertChunkSize {
			sql := fmt.Sprintf(
				"INSERT INTO %s_parent_sums (p_lemma, p_upos, deprel, freq) VALUES %s",
				corpusID, strings.Join(insertPlaceholders, ", "))
			_, err := tx.Exec(sql, args...)
			if err != nil {
				tx.Rollback()
				return err
			}
			args = make([]any, 0, bulkInsertChunkSize*4)
			insertPlaceholders = make([]string, 0, bulkInsertChunkSize)
			i = 0
			log.Debug().Int("items", bulkInsertChunkSize).Msg("written parent Fy bulk into database")
		}

		args = append(args, v.Lemma, v.Upos, v.Deprel, v.Freq)
		insertPlaceholders = append(insertPlaceholders, "(?, ?, ?, ?)")
		i++
	}

	if len(args) > 0 {
		sql := fmt.Sprintf(
			"INSERT INTO %s_parent_sums (p_lemma, p_upos, deprel, freq) VALUES %s",
			corpusID, strings.Join(insertPlaceholders, ", "))
		_, err := tx.Exec(sql, args...)
		if err != nil {
			tx.Rollback()
			return err
		}
		log.Debug().Int("items", len(insertPlaceholders)).Msg("written parent Fy bulk into database")
	}
	return nil
}

func writeChildren(tx *sql.Tx, table FyTable, corpusID string) error {
	var i int
	args := make([]any, 0, bulkInsertChunkSize*4)
	insertPlaceholders := make([]string, 0, bulkInsertChunkSize)

	for _, v := range table {
		if i == bulkInsertChunkSize {
			sql := fmt.Sprintf(
				"INSERT INTO %s_child_sums (lemma, upos, deprel, freq) VALUES %s",
				corpusID, strings.Join(insertPlaceholders, ", "))
			_, err := tx.Exec(sql, args...)
			if err != nil {
				tx.Rollback()
				return err
			}
			args = make([]any, 0, bulkInsertChunkSize*4)
			insertPlaceholders = make([]string, 0, bulkInsertChunkSize)
			i = 0
			log.Debug().Int("items", bulkInsertChunkSize).Msg("written child Fy bulk into database")
		}

		args = append(args, v.Lemma, v.Upos, v.Deprel, v.Freq)
		insertPlaceholders = append(insertPlaceholders, "(?, ?, ?, ?)")
		i++
	}

	if len(args) > 0 {
		sql := fmt.Sprintf(
			"INSERT INTO %s_child_sums (lemma, upos, deprel, freq) VALUES %s",
			corpusID, strings.Join(insertPlaceholders, ", "))
		_, err := tx.Exec(sql, args...)
		if err != nil {
			tx.Rollback()
			return err
		}
		log.Debug().Int("items", len(insertPlaceholders)).Msg("written child Fy bulk into database")
	}
	return nil
}

func runForDeprel(corpusID, vertPath string, coOccSpan int, conf *SyntaxProps, db *sql.DB) error {
	pc := &vertigo.ParserConf{
		InputFilePath:         vertPath,
		Encoding:              "utf-8",
		StructAttrAccumulator: "comb",
	}
	table := make(CounterTable)
	parentSumTable := make(FyTable)
	childSumTable := make(FyTable)
	proc := &VertProcessor{
		DeprelTypes: expandDeprelMultivalues(
			[]string{
				conf.NounModifiedValue,
				conf.NounSubjectValue,
				conf.NounObjectValue,
			},
		),
		conf:         conf,
		Table:        table,
		ParentCounts: parentSumTable,
		ChildCounts:  childSumTable,
	}
	err := vertigo.ParseVerticalFile(pc, proc)
	if err != nil {
		return err
	}

	log.Info().Int("size", len(table)).Msg("collocation table done")

	// prepare only pairs found for syntactic collocations
	// we don't need to know co-occurrences for every possible pair
	coOccTable := make(CoOccTable)
	tokenCounts := make(FyTable)
	for _, v := range table {
		coOccTable.Add(v.Lemma, v.Upos, v.PLemma, v.PUpos, 0)
		tokenCounts.Add(v.Lemma, v.Upos, "", 0)
		tokenCounts.Add(v.PLemma, v.PUpos, "", 0)
	}
	coProc := &CoVertProcessor{
		Span:        coOccSpan,
		conf:        conf,
		CoOccTable:  coOccTable,
		TokenCounts: tokenCounts,
		Window:      make([][2]string, 0, 2*coOccSpan+1),
	}
	err = vertigo.ParseVerticalFile(pc, coProc)
	if err != nil {
		return err
	}

	log.Info().Int("size", len(coOccTable)).Msg("cooccurrence table done")

	ctx := context.Background()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s_fcolls", corpusID))
	if err != nil {
		return err
	}

	t0 := time.Now()

	if err := writeFxy(tx, table, coOccTable, tokenCounts, corpusID); err != nil {
		return err
	}
	if err := writeChildren(tx, childSumTable, corpusID); err != nil {
		return err
	}
	if err := writeParents(tx, parentSumTable, corpusID); err != nil {
		return err
	}

	log.Info().Msg("writing fxy data into database")
	err = tx.Commit()
	if err != nil {
		return err
	}
	log.Info().Float64("durationSec", time.Since(t0).Seconds()).Msg("...writing done")

	return nil
}

func RunPg(corpusID, vertPath string, coOccSpan int, conf *SyntaxProps, db *sql.DB) error {
	return runForDeprel(
		corpusID,
		vertPath,
		coOccSpan,
		conf,
		db,
	)
}
