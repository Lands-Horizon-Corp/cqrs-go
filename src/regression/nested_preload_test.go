package regression

// This file verifies preload resolution at real depth (bun natively
// supports a dotted relation path like "A.B.C" as nested preloading — see
// ValidPreloads/resolveRelationPath in src/utils/preload.go), plus the two
// behaviors layered on top of that: per-segment case/format normalization
// (ToPascalCase), and "drop with a warning, don't fail the read" for any
// segment that doesn't actually name a real relation — e.g. because a
// model was renamed or removed after the preload string was written
// somewhere else in the codebase.
//
// The fixture is a six-level belongs-to chain, five relation hops deep:
// Street -> District -> City -> State -> Country -> Continent.

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/cqrs"
)

type nestedContinent struct {
	bun.BaseModel `bun:"table:nested_continents"`
	ID            string `bun:"id,pk"`
	Name          string `bun:"name"`
}

type nestedCountry struct {
	bun.BaseModel `bun:"table:nested_countries"`
	ID            string           `bun:"id,pk"`
	Name          string           `bun:"name"`
	ContinentID   string           `bun:"continent_id"`
	Continent     *nestedContinent `bun:"rel:belongs-to,join:continent_id=id"`
}

type nestedState struct {
	bun.BaseModel `bun:"table:nested_states"`
	ID            string         `bun:"id,pk"`
	Name          string         `bun:"name"`
	CountryID     string         `bun:"country_id"`
	Country       *nestedCountry `bun:"rel:belongs-to,join:country_id=id"`
}

type nestedCity struct {
	bun.BaseModel `bun:"table:nested_cities"`
	ID            string       `bun:"id,pk"`
	Name          string       `bun:"name"`
	StateID       string       `bun:"state_id"`
	State         *nestedState `bun:"rel:belongs-to,join:state_id=id"`
}

type nestedDistrict struct {
	bun.BaseModel `bun:"table:nested_districts"`
	ID            string      `bun:"id,pk"`
	Name          string      `bun:"name"`
	CityID        string      `bun:"city_id"`
	City          *nestedCity `bun:"rel:belongs-to,join:city_id=id"`
}

type nestedStreet struct {
	bun.BaseModel `bun:"table:nested_streets"`
	ID            string          `bun:"id,pk"`
	Name          string          `bun:"name"`
	DistrictID    string          `bun:"district_id"`
	District      *nestedDistrict `bun:"rel:belongs-to,join:district_id=id"`
}

// continentNameFiveLevelsDown walks the full chain down to Continent.Name,
// returning "" at the first nil link — the test's one-line way to assert
// "the whole five-hop chain loaded" without five separate nil checks.
func continentNameFiveLevelsDown(s *nestedStreet) string {
	if s == nil || s.District == nil || s.District.City == nil ||
		s.District.City.State == nil || s.District.City.State.Country == nil ||
		s.District.City.State.Country.Continent == nil {
		return ""
	}
	return s.District.City.State.Country.Continent.Name
}

func newNestedPreloadTestCQRS(t *testing.T, logs *fakeLogService) (*cqrs.CQRSImpl[nestedStreet, nestedStreet, any, string], *bun.DB) {
	t.Helper()
	sqldb, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	sqldb.SetMaxOpenConns(1)
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	for _, model := range []any{
		(*nestedContinent)(nil), (*nestedCountry)(nil), (*nestedState)(nil),
		(*nestedCity)(nil), (*nestedDistrict)(nil), (*nestedStreet)(nil),
	} {
		if _, err := db.NewCreateTable().Model(model).Exec(ctx); err != nil {
			t.Fatalf("creating table for %T: %v", model, err)
		}
	}

	cfg := cqrs.CQRSImpl[nestedStreet, nestedStreet, any, string]{
		WriteSQLService: &fakeSQLService{db: db},
		ToResource:      func(s *nestedStreet) *nestedStreet { return s },
	}
	if logs != nil {
		cfg.LogService = logs
	}
	return cqrs.NewCQRS(cfg), db
}

// seedFiveLevelChain inserts one full Continent -> Country -> State -> City
// -> District -> Street chain, returning the seeded street's ID.
func seedFiveLevelChain(t *testing.T, db *bun.DB) string {
	t.Helper()
	ctx := context.Background()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("seeding chain: %v", err)
		}
	}
	_, err := db.NewInsert().Model(&nestedContinent{ID: "co1", Name: "Asia"}).Exec(ctx)
	must(err)
	_, err = db.NewInsert().Model(&nestedCountry{ID: "cn1", Name: "Japan", ContinentID: "co1"}).Exec(ctx)
	must(err)
	_, err = db.NewInsert().Model(&nestedState{ID: "st1", Name: "Tokyo-to", CountryID: "cn1"}).Exec(ctx)
	must(err)
	_, err = db.NewInsert().Model(&nestedCity{ID: "ci1", Name: "Shibuya", StateID: "st1"}).Exec(ctx)
	must(err)
	_, err = db.NewInsert().Model(&nestedDistrict{ID: "di1", Name: "Shibuya-ku", CityID: "ci1"}).Exec(ctx)
	must(err)
	_, err = db.NewInsert().Model(&nestedStreet{ID: "sr1", Name: "Dogenzaka", DistrictID: "di1"}).Exec(ctx)
	must(err)
	return "sr1"
}

func TestNestedPreload_HappyPath_FiveLevelsDeepLoadsEntireChain(t *testing.T) {
	t.Parallel()
	c, db := newNestedPreloadTestCQRS(t, nil)
	seedFiveLevelChain(t, db)

	got, err := c.GetByID(context.Background(), "sr1", "District.City.State.Country.Continent")
	if err != nil {
		t.Fatalf("GetByID returned error: %v", err)
	}
	if name := continentNameFiveLevelsDown(got); name != "Asia" {
		t.Fatalf("expected the full 5-hop chain to resolve to Continent.Name \"Asia\", got %q (full struct: %+v)", name, got)
	}
}

// TestNestedPreload_HappyPath_SegmentCaseAndFormatNormalizes proves
// ToPascalCase runs on every segment of a nested path independently: the
// same 5-hop chain, given with each segment in a different casing/format,
// still resolves to the exact relation names bun needs.
func TestNestedPreload_HappyPath_SegmentCaseAndFormatNormalizes(t *testing.T) {
	t.Parallel()
	c, db := newNestedPreloadTestCQRS(t, nil)
	seedFiveLevelChain(t, db)

	cases := []struct {
		name    string
		preload string
	}{
		{"all lowercase", "district.city.state.country.continent"},
		{"mixed case per segment", "District.city.State.country.Continent"},
		{"already-correct PascalCase (unchanged)", "District.City.State.Country.Continent"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := c.GetByID(context.Background(), "sr1", tc.preload)
			if err != nil {
				t.Fatalf("GetByID returned error: %v", err)
			}
			if name := continentNameFiveLevelsDown(got); name != "Asia" {
				t.Fatalf("expected %q to normalize and resolve the full chain, got Continent.Name %q (full struct: %+v)",
					tc.preload, name, got)
			}
		})
	}
}

// TestNestedPreload_HappyPath_BrokenMiddleSegmentDropsOnlyThatEntry proves
// the resilience requirement directly: a preload entry whose middle
// segment doesn't name a real relation (simulating that relation having
// been renamed or removed elsewhere in the codebase) is dropped in its
// entirety — but a second, independent, still-valid preload given in the
// same call is entirely unaffected, and the read itself still succeeds.
func TestNestedPreload_HappyPath_BrokenMiddleSegmentDropsOnlyThatEntry(t *testing.T) {
	t.Parallel()
	logs := &fakeLogService{}
	c, db := newNestedPreloadTestCQRS(t, logs)
	seedFiveLevelChain(t, db)

	got, err := c.GetByID(context.Background(), "sr1",
		"District.City.NotARealRelationAnymore.Country.Continent", // broken chain: dropped whole
		"District", // independent, still valid: unaffected
	)
	if err != nil {
		t.Fatalf("expected the broken chain to be dropped rather than error, got: %v", err)
	}
	if got.District == nil {
		t.Fatalf("expected the independent \"District\" preload to still load, got %+v", got)
	}
	if got.District.City != nil {
		t.Fatalf("expected the broken chain to load nothing beyond District (it was dropped whole), got %+v", got.District)
	}

	waitForWarnLogs(t, logs, 1)
}

// TestNestedPreload_HappyPath_RemovedRelationDoesNotBreakProd is the
// scenario from the request directly: a preload list written when a model
// relation still existed keeps working after that relation is gone — the
// valid 5-hop chain still loads in full, the now-nonexistent one is
// dropped with a warning, and nothing about the call errors.
func TestNestedPreload_HappyPath_RemovedRelationDoesNotBreakProd(t *testing.T) {
	t.Parallel()
	logs := &fakeLogService{}
	c, db := newNestedPreloadTestCQRS(t, logs)
	seedFiveLevelChain(t, db)

	got, err := c.GetByID(context.Background(), "sr1",
		"District.City.State.Country.Continent", // still valid today
		"SomeRelationThatUsedToExist",           // simulates a removed model/relation
	)
	if err != nil {
		t.Fatalf("expected the removed relation to be dropped rather than error, got: %v", err)
	}
	if name := continentNameFiveLevelsDown(got); name != "Asia" {
		t.Fatalf("expected the still-valid chain to load in full despite the other, removed preload, got %q (full struct: %+v)",
			name, got)
	}

	waitForWarnLogs(t, logs, 1)
}

// waitForWarnLogs polls logs (c.warn/pagination's own warn are both
// fire-and-forget goroutines — see cqrs.logger.go/pagination.logger.go) up
// to 2 seconds for at least want "warn"-level entries, the same pattern
// TestPagination_HappyPath_UnknownFilterFieldLogsAWarnWhenDropped uses in
// pagination_normalize_test.go, and fails the test if the deadline passes
// first.
func waitForWarnLogs(t *testing.T, logs *fakeLogService, want int) []string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var msgs []string
		for _, call := range logs.snapshot() {
			if call.level == "warn" {
				msgs = append(msgs, call.msg)
			}
		}
		if len(msgs) >= want {
			return msgs
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected at least %d warn log(s) within 2s, got %v", want, msgs)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
