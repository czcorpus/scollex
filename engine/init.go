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
	"database/sql"
	"fmt"

	"github.com/rs/zerolog/log"
)

const (
	defaultWordColumnSize = 300
)

func (cdb *CollDatabase) dropCollsTable(tx *sql.Tx) error {
	_, err := tx.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s_fcolls`, cdb.corpusID))
	return err
}

func (cdb *CollDatabase) createCollsTable(tx *sql.Tx, vcLen int) error {
	_, err := tx.Exec(fmt.Sprintf(`CREATE TABLE %s_fcolls (
		id int(11) NOT NULL AUTO_INCREMENT,
		lemma varchar(%d) NOT NULL,
		upos varchar(50) NOT NULL,
		p_lemma varchar(%d) NOT NULL,
		p_upos varchar(50) NOT NULL,
		deprel varchar(50) NOT NULL,
		freq int(11) NOT NULL,
		PRIMARY KEY (id)
	  )`, cdb.corpusID, vcLen, vcLen))

	if err != nil {
		return fmt.Errorf("failed to CREATE table %s_fcolls: %w", cdb.corpusID, err)
	}
	return nil
}

func (cdb *CollDatabase) dropParentSumsTable(tx *sql.Tx) error {
	_, err := tx.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s_parent_sums`, cdb.corpusID))
	if err != nil {
		return fmt.Errorf("failed to DROP table %s_parent_sums: %w", cdb.corpusID, err)
	}
	return nil
}

func (cdb *CollDatabase) createParentSumsTable(tx *sql.Tx, vcLen int) error {
	_, err := tx.Exec(fmt.Sprintf(`CREATE TABLE %s_parent_sums (
		id int(11) NOT NULL AUTO_INCREMENT,
		p_lemma varchar(%d) NOT NULL,
		p_upos varchar(50) NOT NULL,
		deprel varchar(50) NOT NULL,
		freq int(11) NOT NULL,
		PRIMARY KEY (id)
	  )`, cdb.corpusID, vcLen))
	if err != nil {
		return fmt.Errorf("failed to CREATE table %s_parent_sums: %w", cdb.corpusID, err)
	}
	return nil
}

func (cdb *CollDatabase) dropChildSumsTable(tx *sql.Tx) error {
	_, err := tx.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s_child_sums`, cdb.corpusID))
	if err != nil {
		return fmt.Errorf("failed to DROP table %s_child_sums: %w", cdb.corpusID, err)
	}
	return nil
}

func (cdb *CollDatabase) createChildSumsTable(tx *sql.Tx, vcLen int) error {
	_, err := tx.Exec(fmt.Sprintf(`CREATE TABLE %s_child_sums (
		id int(11) NOT NULL AUTO_INCREMENT,
		lemma varchar(%d) NOT NULL,
		upos varchar(50) NOT NULL,
		deprel varchar(50) NOT NULL,
		freq int(11) NOT NULL,
		PRIMARY KEY (id)
	)`, cdb.corpusID, vcLen))
	if err != nil {
		return fmt.Errorf("failed to CREATE table %s_child_sums: %w", cdb.corpusID, err)
	}
	return nil
}

func (cdb *CollDatabase) InitializeDB(db *sql.DB, force bool) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	if force {
		log.Info().Msg("dropping existing tables (requested by the -f arg.)")
		err = cdb.dropCollsTable(tx)
		if err != nil {
			tx.Rollback()
			return err
		}
		err = cdb.dropParentSumsTable(tx)
		if err != nil {
			tx.Rollback()
			return err
		}
		err = cdb.dropChildSumsTable(tx)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	log.Info().Msg("creating tables")
	err = cdb.createCollsTable(tx, defaultWordColumnSize)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = cdb.createParentSumsTable(tx, defaultWordColumnSize)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = cdb.createChildSumsTable(tx, defaultWordColumnSize)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}
	return nil
}
