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

import "fmt"

type DBConf struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Name     string `json:"name"`
	User     string `json:"user"`
	Password string `json:"password"`
	PoolSize int    `json:"poolSize"`
}

type PosAttrProps struct {
	Name        string `json:"name"`
	VerticalCol int    `json:"verticalCol"`
}

type CorporaConf []*CorpusProps

func (cp CorporaConf) GetCorpusProps(corpusID string) *CorpusProps {
	for _, props := range cp {
		if props.Name == corpusID {
			return props
		}
	}
	return nil
}

type CorpusProps struct {
	Name   string      `json:"name"`
	Size   int64       `json:"size"`
	Syntax SyntaxProps `json:"syntax"`
}

func (conf *CorpusProps) ValidateAndDefaults(confContext string) error {
	return conf.Syntax.ValidateAndDefaults(confContext)
}

type SyntaxProps struct {

	// ParentIdxAttr specifies a positional attribute providing
	// information about relative position of a parent token.
	ParentIdxAttr PosAttrProps `json:"parentIdxAttr"`

	// LemmaAttr - an attribute specifying lemma
	// (in intercorp_v13ud: `lemma`)
	LemmaAttr PosAttrProps `json:"lemmaAttr"`

	// ParLemmaAttr - an attribute specifying lemma in parent
	// (in intercorp_v13ud: `p_lemma`)
	ParLemmaAttr PosAttrProps `json:"parLemmaAttr"`

	// PosAttr - an attr specifying part of speech
	// (in intercorp_v13ud: `upos`)
	PosAttr PosAttrProps `json:"posAttr"`

	// ParPosAttr - an attr specifying part of speech in parent
	// (in intercorp_v13ud: `p_upos`)
	ParPosAttr PosAttrProps `json:"parPosAttr"`

	// (in intercorp_v13ud: `deprel`)
	FuncAttr PosAttrProps `json:"funcAttr"`

	// (in intercorp_v13ud: `NOUN`)
	NounValue string `json:"nounPosValue"`

	// (in intercorp_v13ud: `VERB`)
	VerbValue string `json:"verbPosValue"`

	// (in intercorp_v13ud: `nmod`)
	NounModifiedValue string `json:"nounModifiedValue"`

	// (in intercorp_v13ud: `nsubj`)
	NounSubjectValue string `json:"nounSubjectValue"`

	// (in intercorp_v13ud: `obj|iobj`)
	NounObjectValue string `json:"nounObjectValue"`
}

func (conf *SyntaxProps) ValidateAndDefaults(confContext string) error {
	if conf.ParentIdxAttr.Name == "" {
		return fmt.Errorf("missing `%s.parentIdxAttr`", confContext)
	}
	if conf.LemmaAttr.Name == "" {
		return fmt.Errorf("missing `%s.lemmaAttr`", confContext)
	}
	if conf.ParLemmaAttr.Name == "" {
		return fmt.Errorf("missing `%s.parLemmaAttr`", confContext)
	}
	if conf.PosAttr.Name == "" {
		return fmt.Errorf("missing `%s.posAttr`", confContext)
	}
	if conf.ParPosAttr.Name == "" {
		return fmt.Errorf("missing `%s.parPosAttr`", confContext)
	}
	if conf.FuncAttr.Name == "" {
		return fmt.Errorf("missing `%s.funcAttr`", confContext)
	}
	if conf.NounValue == "" {
		return fmt.Errorf("missing `%s.nounPosValue`", confContext)
	}
	if conf.VerbValue == "" {
		return fmt.Errorf("missing `%s.verbPosValue`", confContext)
	}
	if conf.NounModifiedValue == "" {
		return fmt.Errorf("missing `%s.nounModifiedValue`", confContext)
	}
	if conf.NounSubjectValue == "" {
		return fmt.Errorf("missing `%s.nounSubjectValue`", confContext)
	}
	if conf.NounObjectValue == "" {
		return fmt.Errorf("missing `%s.nounObjectValue`", confContext)
	}
	return nil
}
