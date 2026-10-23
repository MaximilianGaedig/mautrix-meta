// mautrix-meta - A Matrix-Facebook Messenger and Instagram DM puppeting bridge.
// Copyright (C) 2026 Maximilian Gaedig
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package metadb

// Fork-only schema. Upstream's migrations stay exactly upstream's files with upstream's numbers in
// pkg/metadb/*.sql (version table meta_version); this fork's own tables live in pkg/metadb/fork/*.sql
// with their own numbering in mxg_meta_version. An upstream migration can then never land on a
// number this fork already used, so merging upstream never needs a renumbering (it did twice: our
// 15-polls pushed upstream's 15/16 to 16/17).
//
// Never add a fork migration to pkg/metadb/*.sql: add pkg/metadb/fork/NN-name.sql ("-- vN: ...")
// and bump the fork latest schema instead.

import (
	"context"
	"embed"
	"fmt"
	"io/fs"

	"go.mau.fi/util/dbutil"
)

const forkVersionTable = "mxg_meta_version"

// legacyForkVersion is the meta_version number our polls migration took before it moved here.
// Every meta_version at or above it counts one fork migration that upstream does not have.
const legacyForkVersion = 15

//go:embed fork/*.sql
var forkUpgrades embed.FS

var forkTable = dbutil.BuildUpgradeTable().WithFS(mustSub(forkUpgrades, "fork")).Finish()

// upgradeFS is what dbutil reads an upgrade table from.
type upgradeFS interface {
	fs.ReadDirFS
	fs.ReadFileFS
}

func mustSub(f fs.FS, dir string) upgradeFS {
	sub, err := fs.Sub(f, dir)
	if err != nil {
		panic(err)
	}
	return sub.(upgradeFS)
}

func (db *MetaDB) forkDB() *dbutil.Database {
	return db.Database.Child(forkVersionTable, forkTable, db.Database.Log)
}

// Upgrade runs upstream's migrations, then the fork's. A database from before the split is moved
// to upstream's numbering first, so upstream's own migrations run on it under their own numbers.
func (db *MetaDB) Upgrade(ctx context.Context) error {
	if err := db.legacyForkNumbering(ctx); err != nil {
		return fmt.Errorf("failed to move fork migrations to %s: %w", forkVersionTable, err)
	}
	if err := db.Database.Upgrade(ctx); err != nil {
		return err
	}
	return db.forkDB().Upgrade(ctx)
}

// legacyForkNumbering handles databases where 15-polls ran inside meta_version: meta_poll exists
// but mxg_meta_version does not. In that numbering v15 was polls and every later version was
// upstream's version plus one (16-thread-cutover was upstream's 15, 17-refresh-cutover-cache its
// 16), so meta_version goes down by one and the fork's table is recorded at v1. Nothing else is
// touched. It runs once: afterwards mxg_meta_version exists.
func (db *MetaDB) legacyForkNumbering(ctx context.Context) error {
	if exists, err := db.TableExists(ctx, forkVersionTable); err != nil || exists {
		return err
	}
	if exists, err := db.TableExists(ctx, "meta_poll"); err != nil || !exists {
		// A new database (or one from upstream): nothing to move, the fork schema is created fresh.
		return err
	}
	return db.DoTxn(ctx, nil, func(ctx context.Context) error {
		var version, compat int
		err := db.QueryRow(ctx, "SELECT version, COALESCE(compat, version) FROM meta_version LIMIT 1").Scan(&version, &compat)
		if err != nil {
			return fmt.Errorf("failed to read meta_version: %w", err)
		}
		if version < legacyForkVersion {
			return fmt.Errorf("meta_poll exists but meta_version is v%d, below the fork's v%d", version, legacyForkVersion)
		}
		version--
		compat = min(compat, version)
		if _, err = db.Exec(ctx, "UPDATE meta_version SET version = $1, compat = $2", version, compat); err != nil {
			return fmt.Errorf("failed to renumber meta_version: %w", err)
		}
		if _, err = db.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (version INTEGER, compat INTEGER)", forkVersionTable)); err != nil {
			return err
		}
		_, err = db.Exec(ctx, fmt.Sprintf("INSERT INTO %s (version, compat) VALUES (1, 1)", forkVersionTable))
		return err
	})
}
