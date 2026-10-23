package metadb

import (
	"io/fs"
	"regexp"
	"strconv"
	"testing"
)

var migrationHeader = regexp.MustCompile(`^-- (?:v0 -> )?v(\d+) `)

// The fork adds its own migrations (15-polls) next to upstream's, and a database already at a version never runs
// another file with that number again. Two files claiming one version, or a gap, would silently skip a table on
// databases that are already upgraded, so every version up to the latest schema must be claimed by exactly one file.
func TestMigrationVersionsAreUniqueAndContiguous(t *testing.T) {
	files, err := fs.Glob(upgrades, "*.sql")
	if err != nil {
		t.Fatal(err)
	}
	latest := 0
	owners := map[int]string{}
	for _, name := range files {
		data, err := upgrades.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		m := migrationHeader.FindSubmatch(data)
		if m == nil {
			t.Fatalf("%s: no version header", name)
		}
		version, _ := strconv.Atoi(string(m[1]))
		if name == "00-latest-schema.sql" {
			latest = version
			continue
		}
		if prev, ok := owners[version]; ok {
			t.Errorf("v%d is claimed by both %s and %s", version, prev, name)
		}
		owners[version] = name
		if want := name[:2]; want != strconv.Itoa(version) && want != "0"+strconv.Itoa(version) {
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
	for _, name := range []string{"15-polls.sql", "16-thread-cutover.sql", "17-refresh-cutover-cache.sql"} {
		if _, err := upgrades.ReadFile(name); err != nil {
			t.Errorf("%s missing: %v", name, err)
		}
	}
}
