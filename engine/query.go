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

	"github.com/czcorpus/cnc-gokit/collections"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

type Candidate struct {
	Lemma  string
	Upos   string
	FreqXY int64
	FreqY  int64
}

// CollDatabase
// note: the lifecycle of the instance
// is "per request"
type CollDatabase struct {
	db          *pgxpool.Pool
	useMatViews bool
	corpusID    string
	ctx         context.Context
}

func (cdb *CollDatabase) TableName() string {
	return fmt.Sprintf("%s_fcolls", cdb.corpusID)
}

func (cdb *CollDatabase) TestTableReady() error {
	tx, err := cdb.db.Begin(cdb.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(cdb.ctx)
	cmd, err := cdb.db.Exec(
		cdb.ctx, fmt.Sprintf("INSERT INTO %s_fcolls (id) VALUES (-1)", cdb.corpusID))
	if err != nil {
		return err
	}
	if cmd.RowsAffected() != 1 {
		return fmt.Errorf(
			"problem inserting testing row - num affected rows: %d", cmd.RowsAffected())
	}
	row := cdb.db.QueryRow(
		cdb.ctx, fmt.Sprintf("SELECT id FROM %s_fcolls where id = $1", cdb.corpusID), -1)
	var v sql.NullInt64
	err = row.Scan(&v)
	if err == pgx.ErrNoRows {
		return nil
	}
	return err
}

func (cdb *CollDatabase) GetFreq(lemma, upos, pLemma, pUpos, deprel string) (int64, error) {

	whereSQL := make([]string, 0, 4)
	whereArgs := pgx.NamedArgs{}
	if deprel != "" {
		deprelParsed := strings.Split(deprel, "|")
		deprelArgs := pgx.NamedArgs{}
		deprelSql := make([]string, len(deprelParsed))
		for i, dp := range deprelParsed {
			deprelSql[i] = fmt.Sprintf("deprel = @deprel_%d", i)
			deprelArgs[fmt.Sprintf("deprel_%d", i)] = dp
		}
		whereSQL = append(whereSQL, fmt.Sprintf("(%s)", strings.Join(deprelSql, " OR ")))
		collections.MapUpdate(whereArgs, deprelArgs)
	}
	if lemma != "" {
		whereSQL = append(whereSQL, "lemma = @lemma")
		whereArgs["lemma"] = lemma
	}
	if upos != "" {
		whereSQL = append(whereSQL, "upos = @upos")
		whereArgs["upos"] = upos
	}
	if pLemma != "" {
		whereSQL = append(whereSQL, "p_lemma = @p_lemma")
		whereArgs["p_lemma"] = pLemma
	}
	if pUpos != "" {
		whereSQL = append(whereSQL, "p_upos = @p_upos")
		whereArgs["p_upos"] = pUpos
	}

	sql := fmt.Sprintf("SELECT SUM(freq) FROM %s_fcolls WHERE %s", cdb.corpusID, strings.Join(whereSQL, " AND "))
	log.Debug().Str("sql", sql).Any("args", whereArgs).Msg("going to SELECT cumulative freq.")
	t0 := time.Now()
	row := cdb.db.QueryRow(cdb.ctx, sql, whereArgs)
	var ans int64
	err := row.Scan(&ans)
	if err != nil {
		return 0, err
	}
	log.Debug().Float64("proctime", time.Since(t0).Seconds()).Msg(".... DONE (select cumulative freq.)")
	return ans, nil
}

func (cdb *CollDatabase) GetChildCandidates(pLemma, pUpos, deprel string, minFreq int) ([]*Candidate, error) {
	whereSQL := make([]string, 0, 4)
	whereSQL = append(whereSQL, "p_lemma = @p_lemma", "freq >= @freq")
	whereArgs := pgx.NamedArgs{}
	whereArgs["p_lemma"] = pLemma
	whereArgs["freq"] = minFreq

	if deprel != "" {
		deprelParsed := strings.Split(deprel, "|")
		deprelArgs := pgx.NamedArgs{}
		deprelSql := make([]string, len(deprelParsed))
		for i, dp := range deprelParsed {
			deprelSql[i] = fmt.Sprintf("deprel = @deprel_%d", i)
			deprelArgs[fmt.Sprintf("deprel_%d", i)] = dp
		}
		whereSQL = append(whereSQL, fmt.Sprintf("(%s)", strings.Join(deprelSql, " OR ")))
		collections.MapUpdate(whereArgs, deprelArgs)
	}

	if pUpos != "" {
		whereSQL = append(whereSQL, "p_upos = @p_upos")
		whereArgs["p_upos"] = pUpos
	}
	var sql string
	if cdb.useMatViews {
		sql = fmt.Sprintf(
			"SELECT lemma, upos, freq, fy "+
				"FROM %s_lemma_candidates "+
				"WHERE %s ",
			cdb.corpusID, strings.Join(whereSQL, " AND "),
		)

	} else {
		sql = fmt.Sprintf(
			"SELECT a.lemma, a.upos, a.freq, "+
				"(SELECT SUM(freq) FROM %s_fcolls AS b "+
				" WHERE b.lemma = a.lemma AND b.upos = a.upos AND b.deprel = a.deprel) "+
				"FROM %s_fcolls AS a WHERE %s ",
			cdb.corpusID, cdb.corpusID, strings.Join(whereSQL, " AND "),
		)
	}
	log.Debug().Str("sql", sql).Any("args", whereArgs).Msg("going to SELECT child candidates")
	t0 := time.Now()
	rows, err := cdb.db.Query(cdb.ctx, sql, whereArgs)
	if err != nil {
		return []*Candidate{}, err
	}
	ans := make([]*Candidate, 0, 100)
	for rows.Next() {
		item := &Candidate{}
		err := rows.Scan(&item.Lemma, &item.Upos, &item.FreqXY, &item.FreqY)
		if err != nil {
			return ans, err
		}
		ans = append(ans, item)
	}
	log.Debug().Err(rows.Err()).Float64("proctime", time.Since(t0).Seconds()).Msg(".... DONE (SELECT child candidates)")
	return ans, rows.Err()
}

func (cdb *CollDatabase) GetParentCandidates(lemma, upos, deprel string, minFreq int) ([]*Candidate, error) {
	whereSQL := make([]string, 0, 4)
	whereSQL = append(whereSQL, "lemma = @lemma", "freq >= @freq")
	whereArgs := pgx.NamedArgs{}
	whereArgs["lemma"] = lemma
	whereArgs["freq"] = minFreq

	if deprel != "" {
		deprelParsed := strings.Split(deprel, "|")
		deprelArgs := pgx.NamedArgs{}
		deprelSql := make([]string, len(deprelParsed))
		for i, dp := range deprelParsed {
			deprelSql[i] = fmt.Sprintf("deprel = @deprel_%d", i)
			deprelArgs[fmt.Sprintf("deprel_%d", i)] = dp
		}
		whereSQL = append(whereSQL, fmt.Sprintf("(%s)", strings.Join(deprelSql, " OR ")))
		collections.MapUpdate(whereArgs, deprelArgs)
	}

	if upos != "" {
		whereSQL = append(whereSQL, "upos = @upos")
		whereArgs["upos"] = upos
	}
	var sql string
	if cdb.useMatViews {
		sql = fmt.Sprintf(
			"SELECT p_lemma, p_upos, freq, fy "+
				"FROM %s_p_lemma_candidates "+
				"WHERE %s ",
			cdb.corpusID, strings.Join(whereSQL, " AND "),
		)

	} else {
		sql = fmt.Sprintf(
			"SELECT p_lemma, p_upos, freq, "+
				"(SELECT SUM(freq) FROM %s_fcolls AS b "+
				" WHERE b.p_lemma = a.p_lemma AND b.p_upos = a.p_upos AND b.deprel = a.deprel) "+
				"FROM %s_fcolls AS a WHERE %s ",
			cdb.corpusID, cdb.corpusID, strings.Join(whereSQL, " AND "),
		)
	}
	log.Debug().Str("sql", sql).Any("args", whereArgs).Msg("going to SELECT parent candidates")
	t0 := time.Now()
	rows, err := cdb.db.Query(cdb.ctx, sql, whereArgs)
	if err != nil {
		return []*Candidate{}, err
	}
	ans := make([]*Candidate, 0, 100)
	for rows.Next() {
		item := &Candidate{}
		err := rows.Scan(&item.Lemma, &item.Upos, &item.FreqXY, &item.FreqY)
		if err != nil {
			return ans, err
		}
		ans = append(ans, item)
	}
	log.Debug().Err(rows.Err()).Float64("proctime", time.Since(t0).Seconds()).Msg(".... DONE (SELECT parent candidates)")
	return ans, rows.Err()
}

func NewCollDatabase(db *pgxpool.Pool, corpusID string, useMatViews bool) *CollDatabase {
	return &CollDatabase{
		db:          db,
		useMatViews: useMatViews,
		corpusID:    corpusID,
		ctx:         context.Background(),
	}
}
