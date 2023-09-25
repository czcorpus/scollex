// Copyright 2019 Tomas Machalek <tomas.machalek@gmail.com>
// Copyright 2019 Institute of the Czech National Corpus,
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
