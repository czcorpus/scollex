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

	"github.com/jackc/pgx/v5/pgxpool"
)

func OpenConnection(conf *DBConf, ctx context.Context) (*pgxpool.Pool, error) {
	dsn := fmt.Sprintf( // TODO  pool_max_conns=%d
		"user=%s password=%s host=%s port=%d dbname=%s sslmode=disable",
		conf.User, conf.Password, conf.Host, conf.Port, conf.Name,
	)
	return pgxpool.New(ctx, dsn)
}
