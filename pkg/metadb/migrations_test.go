package metadb

import (
	"context"
	"database/sql"
	"io/fs"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2/database"
)

var migrationHeader = regexp.MustCompile(`^-- (?:v0 -> )?v(\d+)[ :]`)

var createTable = regexp.MustCompile(`(?im)^\s*CREATE TABLE (?:IF NOT EXISTS )?(\w+)`)

// checkLine checks one upgrade line: every version up to the latest schema is claimed by exactly one
// file, and the file prefix matches its header. A gap or a double claim silently skips a migration
// on databases that are already past that number.
func checkLine(t *testing.T, files fs.FS) (creates []string) {
	t.Helper()
	names, err := fs.Glob(files, "*.sql")
	if err != nil {
		t.Fatal(err)
	}
	latest := 0
	owners := map[int]string{}
	for _, name := range names {
		data, err := fs.ReadFile(files, name)
		if err != nil {
			t.Fatal(err)
		}
		m := migrationHeader.FindSubmatch(data)
		if m == nil {
			t.Fatalf("%s: no version header", name)
		}
		version, _ := strconv.Atoi(string(m[1]))
		for _, c := range createTable.FindAllSubmatch(data, -1) {
			creates = append(creates, string(c[1]))
		}
		if name == "00-latest-schema.sql" {
			latest = version
			continue
		}
		if prev, ok := owners[version]; ok {
			t.Errorf("v%d is claimed by both %s and %s", version, prev, name)
		}
		owners[version] = name
		if want := strings.TrimLeft(name[:2], "0"); want != strconv.Itoa(version) {
			t.Errorf("%s: file prefix does not match header v%d", name, version)
		}
	}
	if latest == 0 {
		t.Fatal("00-latest-schema.sql has no version")
	}
	for v := 2; v <= latest; v++ {
		if _, ok := owners[v]; !ok {
			t.Errorf("no migration for v%d (latest schema is v%d)", v, latest)
		}
	}
	for v := range owners {
		if v > latest {
			t.Errorf("migration v%d is newer than the latest schema v%d", v, latest)
		}
	}
	return creates
}

// Upstream's line must stay upstream's: a fork table created in pkg/metadb/*.sql takes a version
// number that upstream's next migration will also take, and then one of the two never runs.
func TestForkTablesStayOutOfUpstreamsLine(t *testing.T) {
	upstreamCreates := checkLine(t, upgrades)
	forkCreates := checkLine(t, mustSub(forkUpgrades, "fork"))
	if len(forkCreates) == 0 {
		t.Error("the fork line creates no tables")
	}
	for _, fork := range forkCreates {
		for _, up := range upstreamCreates {
			if strings.EqualFold(fork, up) {
				t.Errorf("fork table %s is created in upstream's migrations (pkg/metadb/*.sql); it belongs in pkg/metadb/fork/", fork)
			}
		}
	}
}

// newTestDB returns the bridge's base database with bridgev2's tables, which the meta tables reference.
func newTestDB(t *testing.T) (*dbutil.Database, context.Context) {
	t.Helper()
	ctx := context.Background()
	raw, err := sql.Open("sqlite3", "file:"+strings.ReplaceAll(t.Name(), "/", "_")+"?mode=memory&cache=shared&_foreign_keys=on")
	if err != nil {
		t.Fatal(err)
	}
	raw.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = raw.Close() })
	base, err := dbutil.NewWithDB(raw, "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	base.Owner = "mautrix-meta-test"
	if err = database.New("meta", database.MetaTypes{}, base).Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	return base, ctx
}

func upgraded(t *testing.T, ctx context.Context, base *dbutil.Database) *MetaDB {
	t.Helper()
	db := New("meta", base, zerolog.Nop())
	if err := db.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

func versionOf(t *testing.T, ctx context.Context, db *MetaDB, table string) (version, compat int) {
	t.Helper()
	if err := db.QueryRow(ctx, "SELECT version, compat FROM "+table).Scan(&version, &compat); err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	return
}

func mustExec(t *testing.T, ctx context.Context, db *MetaDB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(ctx, query, args...); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

func tableExists(t *testing.T, ctx context.Context, db *MetaDB, name string) bool {
	t.Helper()
	exists, err := db.TableExists(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	return exists
}

// The live database before MEO-199: 15-polls ran as v15 inside meta_version, and upstream's
// 15-thread-cutover and 16-refresh-cutover-cache never ran. After the upgrade it must be on
// upstream's numbering, with upstream's migrations applied and the poll rows kept.
func TestLegacyV15DatabaseMovesToUpstreamNumbering(t *testing.T) {
	base, ctx := newTestDB(t)
	db := upgraded(t, ctx, base)
	// Rebuild the legacy state from the upgraded one.
	mustExec(t, ctx, db, "DROP TABLE IF EXISTS "+forkVersionTable)
	mustExec(t, ctx, db, "DROP TABLE meta_thread_cutover")
	mustExec(t, ctx, db, "UPDATE meta_version SET version = 15, compat = 11")
	mustExec(t, ctx, db, "INSERT INTO meta_poll (bridge_id, poll_id, thread_key, message_id) VALUES ('meta', 1, 2, '$poll')")

	db = upgraded(t, ctx, base)
	if v, c := versionOf(t, ctx, db, "meta_version"); v != 16 || c != 11 {
		t.Errorf("meta_version = v%d (compat v%d), want upstream's v16 (compat v11)", v, c)
	}
	if v, _ := versionOf(t, ctx, db, forkVersionTable); v != 1 {
		t.Errorf("%s = v%d, want v1", forkVersionTable, v)
	}
	if !tableExists(t, ctx, db, "meta_thread_cutover") {
		t.Error("upstream's 15-thread-cutover did not run")
	}
	if _, msg, err := db.GetPoll(ctx, 1); err != nil || msg != "$poll" {
		t.Errorf("poll row lost: %q, %v", msg, err)
	}
}

// The renumbered state (MEO-197 PR #3 deployed): polls v15, upstream's 15/16 ran as 16/17. Nothing
// may run twice, and the numbering must come back to upstream's.
func TestLegacyV17DatabaseMovesToUpstreamNumbering(t *testing.T) {
	base, ctx := newTestDB(t)
	db := upgraded(t, ctx, base)
	mustExec(t, ctx, db, "DROP TABLE IF EXISTS "+forkVersionTable)
	mustExec(t, ctx, db, "UPDATE meta_version SET version = 17, compat = 11")

	db = upgraded(t, ctx, base)
	if v, _ := versionOf(t, ctx, db, "meta_version"); v != 16 {
		t.Errorf("meta_version = v%d, want upstream's v16", v)
	}
	if v, _ := versionOf(t, ctx, db, forkVersionTable); v != 1 {
		t.Errorf("%s = v%d, want v1", forkVersionTable, v)
	}
}

func withExtraFile(t *testing.T, base fs.FS, name, content string) fstest.MapFS {
	t.Helper()
	out := fstest.MapFS{}
	names, err := fs.Glob(base, "*.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		data, err := fs.ReadFile(base, n)
		if err != nil {
			t.Fatal(err)
		}
		out[n] = &fstest.MapFile{Data: data}
	}
	out[name] = &fstest.MapFile{Data: []byte(content)}
	return out
}

// The collision this layout exists for: upstream ships its next migration under the next number of
// its own line while the fork has migrations of its own. On an upgraded database (the fork's
// migrations applied) both upstream's new migration and the fork's next one must run.
func TestUpstreamNextMigrationRunsNextToForkMigrations(t *testing.T) {
	base, ctx := newTestDB(t)
	upgraded(t, ctx, base)

	next := strconv.Itoa(len(table) + 1)
	forkNext := strconv.Itoa(len(forkTable) + 1)
	origTable, origForkTable := table, forkTable
	t.Cleanup(func() { table, forkTable = origTable, origForkTable })
	table = dbutil.BuildUpgradeTable().WithFS(withExtraFile(t, upgrades, next+"-upstream-probe.sql",
		"-- v"+next+" (compatible with v11+): upstream's next migration\nCREATE TABLE upstream_probe (x INTEGER);\n")).Finish()
	forkTable = dbutil.BuildUpgradeTable().WithFS(withExtraFile(t, mustSub(forkUpgrades, "fork"), "0"+forkNext+"-fork-probe.sql",
		"-- v"+forkNext+": the fork's next migration\nCREATE TABLE fork_probe (x INTEGER);\n")).Finish()

	db := upgraded(t, ctx, base)
	for _, name := range []string{"upstream_probe", "fork_probe", "meta_poll", "meta_thread_cutover"} {
		if !tableExists(t, ctx, db, name) {
			t.Errorf("%s missing after the upgrade", name)
		}
	}
}

func TestNewDatabaseGetsBothLines(t *testing.T) {
	base, ctx := newTestDB(t)
	db := upgraded(t, ctx, base)
	if v, _ := versionOf(t, ctx, db, "meta_version"); v != len(table) {
		t.Errorf("meta_version = v%d, want v%d", v, len(table))
	}
	if v, _ := versionOf(t, ctx, db, forkVersionTable); v != len(forkTable) {
		t.Errorf("%s = v%d, want v%d", forkVersionTable, v, len(forkTable))
	}
	if !tableExists(t, ctx, db, "meta_poll") || !tableExists(t, ctx, db, "meta_thread_cutover") {
		t.Error("tables missing on a new database")
	}
	upgraded(t, ctx, base) // a second start changes nothing and does not fail
}
