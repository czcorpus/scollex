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
	"fmt"
	"strings"
	"time"

	"github.com/czcorpus/cnc-gokit/collections"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
	"github.com/tomachalek/vertigo/v5"
)

const (
	bulkInsertChunkSize = 1000
)

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

type VertProcessor struct {
	DeprelCol   int
	DeprelTypes []string
	conf        *SyntaxProps
	Table       CounterTable
}

func expandDeprelMultivalue(value string) []string {
	ans := make([]string, 0, 2)
	tmp := strings.Split(value, "|")
	if len(tmp) > 2 {
		log.Warn().
			Str("expression", value).
			Msg("deprel expression not fully supported")
	}
	ans = append(ans, tmp...)
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
	pUpos := token.Attrs[vp.conf.ParPosAttr.VerticalCol-1]
	pLemma := token.Attrs[vp.conf.ParLemmaAttr.VerticalCol-1]
	for _, deprel := range expandDeprelMultivalue(deprelTmp) {
		if collections.SliceContains(vp.DeprelTypes, deprel) {
			vp.Table.Add(lemma, upos, pLemma, pUpos, deprel, 1)
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

// TODO: update intercorp_v13ud_cs_fcolls set chunk = (FLOOR( 1 + RAND( ) *32))

func runForDeprel(corpusID, vertPath string, conf *SyntaxProps, db *pgxpool.Pool) error {
	pc := &vertigo.ParserConf{
		InputFilePath:         vertPath,
		Encoding:              "utf-8",
		StructAttrAccumulator: "comb",
	}
	table := make(CounterTable)
	proc := &VertProcessor{
		DeprelTypes: expandDeprelMultivalues(
			[]string{
				conf.NounModifiedValue,
				conf.NounSubjectValue,
				conf.NounObjectValue,
			},
		),
		conf:  conf,
		Table: table,
	}
	err := vertigo.ParseVerticalFile(pc, proc)
	if err != nil {
		return err
	}

	log.Info().Int("size", len(table)).Msg("collocation table done")

	ctx := context.Background()

	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}

	_, err = db.Exec(ctx, fmt.Sprintf("DELETE FROM %s_fcolls", corpusID))
	if err != nil {
		return err
	}

	i := 0
	args := make([][]any, 0, bulkInsertChunkSize)
	cols := []string{"lemma", "upos", "p_lemma", "p_upos", "deprel", "freq"}

	log.Info().Msg("writing data into database")
	t0 := time.Now()

	for _, v := range table {
		if i == bulkInsertChunkSize {
			copyCount, err := db.CopyFrom(
				ctx,
				pgx.Identifier{fmt.Sprintf("%s_fcolls", corpusID)},
				cols,
				pgx.CopyFromRows(args),
			)
			if err != nil {
				tx.Rollback(ctx)
				return err
			}
			args = make([][]any, 0, bulkInsertChunkSize)
			i = 0
			log.Debug().Int64("items", copyCount).Msg("written bulk into database")
		}

		args = append(args, []any{v.Lemma, v.Upos, v.PLemma, v.PUpos, v.Deprel, v.Freq})
		i++
	}

	if len(args) > 0 {
		copyCount, err := db.CopyFrom(
			ctx,
			pgx.Identifier{fmt.Sprintf("%s_fcolls", corpusID)},
			cols,
			pgx.CopyFromRows(args),
		)
		if err != nil {
			tx.Rollback(ctx)
			return err
		}
		log.Debug().Int64("items", copyCount).Msg("written bulk into database")
	}
	err = tx.Commit(ctx)
	log.Info().Float64("durationSec", time.Since(t0).Seconds()).Msg("...writing done")
	return err
}

func RunPg(corpusID, vertPath string, conf *SyntaxProps, db *pgxpool.Pool) error {
	return runForDeprel(
		corpusID,
		vertPath,
		conf,
		db,
	)
}
