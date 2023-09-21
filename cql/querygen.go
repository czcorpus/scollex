// Copyright 2019 Tomas Machalek <tomas.machalek@gmail.com>
// Copyright 2019 Institute of the Czech National Corpus,
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

package cql

import (
	"fmt"

	"github.com/czcorpus/scollex/engine"
)

func NounsModifiedBy(conf *engine.SyntaxProps, word engine.Word, collCandidate string) string {
	if word.PoS == "" {
		return fmt.Sprintf(
			"[%s=\"%s\" & %s=\"%s\" & %s=\"%s\" & %s=\"%s\"]",
			conf.LemmaAttr.Name, word.V,
			conf.ParLemmaAttr.Name, collCandidate,
			conf.FuncAttr.Name, conf.NounModifiedValue,
			conf.ParPosAttr.Name, conf.NounValue,
		)
	}
	return fmt.Sprintf(
		"[%s=\"%s\" & %s=\"%s\" & %s=\"%s\" & %s=\"%s\" & %s=\"%s\"]",
		conf.LemmaAttr.Name, word.V,
		conf.PosAttr.Name, word.PoS,
		conf.ParLemmaAttr.Name, collCandidate,
		conf.FuncAttr.Name, conf.NounModifiedValue,
		conf.ParPosAttr.Name, conf.NounValue,
	)
}

func ModifiersOf(conf *engine.SyntaxProps, word engine.Word, collCandidate string) string {
	if word.PoS == "" {
		return fmt.Sprintf(
			"[%s=\"%s\" & %s=\"%s\" & %s=\"%s\" & %s=\"%s\"]",
			conf.ParLemmaAttr.Name, word.V,
			conf.FuncAttr.Name, conf.NounModifiedValue,
			conf.PosAttr.Name, conf.NounValue,
			conf.LemmaAttr.Name, collCandidate,
		)
	}
	return fmt.Sprintf(
		"[%s=\"%s\" & %s=\"%s\" & %s=\"%s\" & %s=\"%s\" & %s=\"%s\"]",
		conf.ParLemmaAttr.Name, word.V,
		conf.ParPosAttr.Name, word.PoS,
		conf.FuncAttr.Name, conf.NounModifiedValue,
		conf.PosAttr.Name, conf.NounValue,
		conf.LemmaAttr.Name, collCandidate,
	)
}

func VerbsObject(conf *engine.SyntaxProps, word engine.Word, collCandidate string) string {
	if word.PoS == "" {
		return fmt.Sprintf(
			`[%s="%s" & %s="%s" & %s="%s" & %s="%s"]`,
			conf.LemmaAttr.Name, word.V,
			conf.FuncAttr.Name, conf.NounObjectValue,
			conf.ParPosAttr.Name, conf.VerbValue,
			conf.ParLemmaAttr.Name, collCandidate,
		)
	}
	return fmt.Sprintf(
		`[%s="%s" & %s="%s" & %s="%s" & %s="%s" & %s="%s"]`,
		conf.LemmaAttr.Name, word.V,
		conf.PosAttr.Name, word.PoS,
		conf.FuncAttr.Name, conf.NounObjectValue,
		conf.ParPosAttr.Name, conf.VerbValue,
		conf.ParLemmaAttr.Name, collCandidate,
	)
}

func VerbsSubject(conf *engine.SyntaxProps, word engine.Word, collCandidate string) string {
	if word.PoS == "" {
		return fmt.Sprintf(
			`[%s="%s" & %s="%s" & %s="%s" & %s="%s"]`,
			conf.LemmaAttr.Name, word.V,
			conf.FuncAttr.Name, conf.NounSubjectValue,
			conf.ParPosAttr.Name, conf.VerbValue,
			conf.ParLemmaAttr.Name, collCandidate,
		)
	}
	return fmt.Sprintf(
		`[%s="%s" & %s="%s" & %s="%s" & %s="%s" & %s="%s"]`,
		conf.LemmaAttr.Name, word.V,
		conf.PosAttr.Name, word.PoS,
		conf.FuncAttr.Name, conf.NounSubjectValue,
		conf.ParPosAttr.Name, conf.VerbValue,
		conf.ParLemmaAttr.Name, collCandidate,
	)
}
