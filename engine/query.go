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
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

type Candidate struct {
	Lemma      string
	Upos       string
	FreqXY     int64
	FreqY      int64
	CoOccScore float64
}

// CollDatabase
// note: the lifecycle of the instance
// is "per request"
type CollDatabase struct {
	db       *sql.DB
	corpusID string
	ctx      context.Context
}

func (cdb *CollDatabase) TableName() string {
	return fmt.Sprintf("%s_fcolls", cdb.corpusID)
}

func (cdb *CollDatabase) TestTableReady() error {
	tx, err := cdb.db.BeginTx(cdb.ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = cdb.db.ExecContext(
		cdb.ctx, fmt.Sprintf("INSERT IGNORE INTO %s_fcolls (id) VALUES (-1)", cdb.corpusID))
	if err != nil {
		return err
	}
	row := cdb.db.QueryRowContext(
		cdb.ctx, fmt.Sprintf("SELECT id FROM %s_fcolls where id = ?", cdb.corpusID), -1)
	var v sql.NullInt64
	err = row.Scan(&v)
	if err == sql.ErrNoRows {
		return nil
	}
	return err
}

func (cdb *CollDatabase) GetFreq(lemma, upos, pLemma, pUpos, deprel string) (int64, error) {

	whereSQL := make([]string, 0, 4)
	whereArgs := make([]any, 0, 10)
	if deprel != "" {
		deprelParsed := strings.Split(deprel, "|")
		deprelArgs := make([]any, len(deprelParsed))
		deprelSql := make([]string, len(deprelParsed))
		for i, dp := range deprelParsed {
			deprelSql[i] = "deprel = ?"
			deprelArgs[i] = dp
		}
		whereSQL = append(whereSQL, fmt.Sprintf("(%s)", strings.Join(deprelSql, " OR ")))
		whereArgs = append(whereArgs, deprelArgs...)
	}
	if lemma != "" {
		whereSQL = append(whereSQL, "lemma = ?")
		whereArgs = append(whereArgs, lemma)
	}
	if upos != "" {
		whereSQL = append(whereSQL, "upos = ?")
		whereArgs = append(whereArgs, upos)
	}
	if pLemma != "" {
		whereSQL = append(whereSQL, "p_lemma = ?")
		whereArgs = append(whereArgs, pLemma)
	}
	if pUpos != "" {
		whereSQL = append(whereSQL, "p_upos = ?")
		whereArgs = append(whereArgs, pUpos)
	}

	sql := fmt.Sprintf("SELECT COALESCE(SUM(freq), 0) FROM %s_fcolls WHERE %s", cdb.corpusID, strings.Join(whereSQL, " AND "))
	log.Debug().Str("sql", sql).Any("args", whereArgs).Msg("going to SELECT cumulative freq.")
	t0 := time.Now()
	row := cdb.db.QueryRowContext(cdb.ctx, sql, whereArgs...)
	var ans int64
	err := row.Scan(&ans)
	if err != nil {
		return 0, err
	}
	log.Debug().Float64("proctime", time.Since(t0).Seconds()).Msg(".... DONE (select cumulative freq.)")
	return ans, nil
}

// GetCollCandidatesOfChild provides collocation candidates of a child
func (cdb *CollDatabase) GetCollCandidatesOfChild(lemma, upos, deprel string, minFreq int) ([]*Candidate, error) {
	mkerr := func(err error) error { return fmt.Errorf("failed to get coll candidates of child: %w", err) }
	whereSQL := make([]string, 0, 4)
	whereSQL = append(whereSQL, "lemma = ?", "freq >= ?")
	whereArgs := make([]any, 0, 4)
	whereArgs = append(whereArgs, lemma, minFreq)
	var deprelSQL []string
	var deprelArgs []any

	if deprel != "" {
		deprelParsed := strings.Split(deprel, "|")
		deprelArgs = make([]any, len(deprelParsed))
		deprelSQL = make([]string, len(deprelParsed))
		for i, dp := range deprelParsed {
			deprelSQL[i] = "deprel = ?"
			deprelArgs[i] = dp
		}
		whereSQL = append(whereSQL, fmt.Sprintf("(%s)", strings.Join(deprelSQL, " OR ")))
		whereArgs = append(whereArgs, deprelArgs...)

	} else {
		deprelSQL = []string{"1 = 1"}
	}

	if upos != "" {
		whereSQL = append(whereSQL, "upos = ?")
		whereArgs = append(whereArgs, upos)
	}

	sql1 := fmt.Sprintf(
		"SELECT p_lemma, p_upos, freq, co_occurrence_score "+
			"FROM %s_fcolls "+
			"WHERE %s ",
		cdb.corpusID, strings.Join(whereSQL, " AND "),
	)
	log.Debug().Str("sql", sql1).Any("args", whereArgs).Msg("going to SELECT child candidates")
	t0 := time.Now()
	rows, err := cdb.db.QueryContext(cdb.ctx, sql1, whereArgs...)
	if err != nil {
		return []*Candidate{}, mkerr(err)
	}
	ans := make([]*Candidate, 0, 100)
	for rows.Next() {
		item := &Candidate{}
		err := rows.Scan(&item.Lemma, &item.Upos, &item.FreqXY, &item.CoOccScore)
		if err != nil {
			return ans, mkerr(err)
		}

		sql2 := fmt.Sprintf(
			"SELECT COALESCE(SUM(freq), 0) "+
				"FROM %s_parent_sums "+
				"WHERE p_lemma = ? AND p_upos = ? AND (%s) ",
			cdb.corpusID, strings.Join(deprelSQL, " OR "))
		whereArgs := append([]any{item.Lemma, item.Upos}, deprelArgs...)
		rows2 := cdb.db.QueryRowContext(
			cdb.ctx, sql2, whereArgs...)
		var fy int64
		err = rows2.Scan(&fy)
		if err != nil {
			return []*Candidate{}, mkerr(err)
		}
		item.FreqY = fy
		ans = append(ans, item)
	}
	log.Debug().Err(rows.Err()).Float64("proctime", time.Since(t0).Seconds()).Msg(".... DONE (SELECT child candidates)")
	return ans, nil
}

// GetCollCandidatesOfParent provides collocation candidates of a parent
func (cdb *CollDatabase) GetCollCandidatesOfParent(lemma, upos, deprel string, minFreq int) ([]*Candidate, error) {
	mkerr := func(err error) error { return fmt.Errorf("failed to get coll candidates of parent: %w", err) }
	whereSQL := make([]string, 0, 4)
	whereSQL = append(whereSQL, "p_lemma = ?", "freq >= ?")
	whereArgs := make([]any, 0, 4)
	whereArgs = append(whereArgs, lemma, minFreq)
	var deprelSQL []string
	var deprelArgs []any

	if deprel != "" {
		deprelParsed := strings.Split(deprel, "|")
		deprelArgs = make([]any, len(deprelParsed))
		deprelSQL = make([]string, len(deprelParsed))
		for i, dp := range deprelParsed {
			deprelSQL[i] = "deprel = ?"
			deprelArgs[i] = dp
		}
		whereSQL = append(whereSQL, fmt.Sprintf("(%s)", strings.Join(deprelSQL, " OR ")))
		whereArgs = append(whereArgs, deprelArgs...)

	} else {
		deprelSQL = []string{"1 = 1"}
	}
	if upos != "" {
		whereSQL = append(whereSQL, "p_upos = ?")
		whereArgs = append(whereArgs, upos)
	}
	sql1 := fmt.Sprintf(
		"SELECT lemma, upos, freq, co_occurrence_score "+
			"FROM %s_fcolls "+
			"WHERE %s ",
		cdb.corpusID, strings.Join(whereSQL, " AND "),
	)
	log.Debug().Str("sql", sql1).Any("args", whereArgs).Msg("going to SELECT child candidates")
	t0 := time.Now()
	rows, err := cdb.db.QueryContext(cdb.ctx, sql1, whereArgs...)
	if err != nil {
		return []*Candidate{}, mkerr(err)
	}
	ans := make([]*Candidate, 0, 100)
	for rows.Next() {
		item := &Candidate{}
		err := rows.Scan(&item.Lemma, &item.Upos, &item.FreqXY, &item.CoOccScore)
		if err != nil {
			return ans, mkerr(err)
		}
		sql2 := fmt.Sprintf(
			"SELECT COALESCE(SUM(freq), 0) "+
				"FROM %s_child_sums "+
				"WHERE lemma = ? AND upos = ? AND %s ",
			cdb.corpusID, strings.Join(deprelSQL, " OR "))
		whereArgs := append([]any{item.Lemma, item.Upos}, deprelArgs...)
		rows2 := cdb.db.QueryRowContext(
			cdb.ctx, sql2, whereArgs...)
		var fy int64
		err = rows2.Scan(&fy)
		if err != nil {
			return []*Candidate{}, mkerr(err)
		}
		item.FreqY = fy

		ans = append(ans, item)
	}
	log.Debug().Err(rows.Err()).Float64("proctime", time.Since(t0).Seconds()).Msg(".... DONE (SELECT parent candidates)")
	return ans, nil
}

func NewCollDatabase(db *sql.DB, corpusID string) *CollDatabase {
	return &CollDatabase{
		db:       db,
		corpusID: corpusID,
		ctx:      context.Background(),
	}
}
