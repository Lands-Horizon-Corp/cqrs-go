package regression

import (
	"context"
	"database/sql"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/cqrs"
)

type preloadAuthor struct {
	bun.BaseModel `bun:"table:preload_authors"`
	ID            string `bun:"id,pk"`
	Name          string `bun:"name"`
}

type preloadPost struct {
	bun.BaseModel `bun:"table:preload_posts"`
	ID            string         `bun:"id,pk"`
	Title         string         `bun:"title,notnull"`
	AuthorID      string         `bun:"author_id"`
	Author        *preloadAuthor `bun:"rel:belongs-to,join:author_id=id"`
}

type preloadPostResource struct {
	ID         string
	Title      string
	AuthorName string // "" whenever Author wasn't preloaded
}

func preloadPostToResource(p *preloadPost) *preloadPostResource {
	res := &preloadPostResource{ID: p.ID, Title: p.Title}
	if p.Author != nil {
		res.AuthorName = p.Author.Name
	}
	return res
}

// newPreloadTestCQRS builds a fresh in-memory SQLite-backed engine plus the
// raw *bun.DB (so tests can seed an author directly), with defaultPreloads
// applied to CQRSImpl.Preloads so the "no args -> use the instance default"
// branch of Preload can be exercised too.
func newPreloadTestCQRS(t *testing.T, defaultPreloads ...string) (*cqrs.CQRSImpl[preloadPost, preloadPostResource, any, string], *bun.DB) {
	t.Helper()
	sqldb, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	sqldb.SetMaxOpenConns(1)
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	if _, err := db.NewCreateTable().Model((*preloadAuthor)(nil)).Exec(ctx); err != nil {
		t.Fatalf("creating preload_authors table: %v", err)
	}
	if _, err := db.NewCreateTable().Model((*preloadPost)(nil)).Exec(ctx); err != nil {
		t.Fatalf("creating preload_posts table: %v", err)
	}

	c := cqrs.NewCQRS(cqrs.CQRSImpl[preloadPost, preloadPostResource, any, string]{
		WriteSQLService: &fakeSQLService{db: db},
		ToResource:      preloadPostToResource,
		Preloads:        defaultPreloads,
	})
	return c, db
}

func seedPreloadAuthor(t *testing.T, db *bun.DB, id, name string) {
	t.Helper()
	if _, err := db.NewInsert().Model(&preloadAuthor{ID: id, Name: name}).Exec(context.Background()); err != nil {
		t.Fatalf("seeding author: %v", err)
	}
}

func TestPreload_HappyPath_CreateWithExplicitPreloadLoadsRelation(t *testing.T) {
	t.Parallel()
	c, db := newPreloadTestCQRS(t)
	ctx := context.Background()
	seedPreloadAuthor(t, db, "a1", "Ada")

	res, err := c.CreateFormat(ctx, preloadPost{ID: "p1", Title: "Hello", AuthorID: "a1"}, "Author")
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if res.AuthorName != "Ada" {
		t.Errorf("expected preloaded author name 'Ada', got %q", res.AuthorName)
	}
}

func TestPreload_HappyPath_CreateWithoutPreloadLeavesRelationEmpty(t *testing.T) {
	t.Parallel()
	c, db := newPreloadTestCQRS(t)
	ctx := context.Background()
	seedPreloadAuthor(t, db, "a1", "Ada")

	res, err := c.CreateFormat(ctx, preloadPost{ID: "p1", Title: "Hello", AuthorID: "a1"})
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if res.AuthorName != "" {
		t.Errorf("expected no relation loaded without an explicit preload, got %q", res.AuthorName)
	}
}

func TestPreload_HappyPath_InstanceDefaultPreloadsAppliesWhenNoArgsGiven(t *testing.T) {
	t.Parallel()
	c, db := newPreloadTestCQRS(t, "Author") // CQRSImpl.Preloads set at construction
	ctx := context.Background()
	seedPreloadAuthor(t, db, "a1", "Ada")

	res, err := c.CreateFormat(ctx, preloadPost{ID: "p1", Title: "Hello", AuthorID: "a1"})
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if res.AuthorName != "Ada" {
		t.Errorf("expected the instance-level default preload to apply, got AuthorName %q", res.AuthorName)
	}
}

func TestPreload_PoisonPill_ExplicitEmptyStringSuppressesInstanceDefault(t *testing.T) {
	t.Parallel()
	c, db := newPreloadTestCQRS(t, "Author") // instance default would normally preload Author
	ctx := context.Background()
	seedPreloadAuthor(t, db, "a1", "Ada")

	// Passing "" is Preload's documented escape hatch for "no preloads for
	// this call", even though the instance has a default configured.
	res, err := c.CreateFormat(ctx, preloadPost{ID: "p1", Title: "Hello", AuthorID: "a1"}, "")
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if res.AuthorName != "" {
		t.Errorf(`expected passing "" to suppress the instance default preload, got AuthorName %q`, res.AuthorName)
	}
}

func TestPreload_HappyPath_UpdateByIDWithExplicitPreloadLoadsRelation(t *testing.T) {
	t.Parallel()
	c, db := newPreloadTestCQRS(t)
	ctx := context.Background()
	seedPreloadAuthor(t, db, "a1", "Ada")
	seedPreloadAuthor(t, db, "a2", "Grace")
	if _, err := c.CreateFormat(ctx, preloadPost{ID: "p1", Title: "Hello", AuthorID: "a1"}); err != nil {
		t.Fatalf("seed Create returned error: %v", err)
	}

	res, err := c.UpdateByIDFormat(ctx, "p1", preloadPost{ID: "p1", Title: "Updated", AuthorID: "a2"}, "Author")
	if err != nil {
		t.Fatalf("UpdateByID returned error: %v", err)
	}
	if res.Title != "Updated" {
		t.Errorf("expected title 'Updated', got %q", res.Title)
	}
	if res.AuthorName != "Grace" {
		t.Errorf("expected preloaded author name 'Grace' (the new author), got %q", res.AuthorName)
	}
}

// TestPreload_HappyPath_UnknownRelationNameIsDroppedNotAnError documents the
// current, deliberate behavior: an unknown preload relation name (a typo,
// or one that was renamed/removed from the model since the caller was
// written) is dropped with a warning rather than failing the whole
// operation — a stale preload shouldn't be able to take a read down in
// production just because a model changed shape elsewhere.
func TestPreload_HappyPath_UnknownRelationNameIsDroppedNotAnError(t *testing.T) {
	t.Parallel()
	c, db := newPreloadTestCQRS(t)
	ctx := context.Background()
	seedPreloadAuthor(t, db, "a1", "Ada")

	res, err := c.CreateFormat(ctx, preloadPost{ID: "p1", Title: "Hello", AuthorID: "a1"}, "NotARealRelation")
	if err != nil {
		t.Fatalf("expected the unknown relation to be dropped rather than error, got: %v", err)
	}
	if res.AuthorName != "" {
		t.Errorf("expected Author not to be loaded (the only preload given was unknown), got %q", res.AuthorName)
	}
}

func TestPreload_HappyPath_CreateManyLoadsRelationForEveryRecord(t *testing.T) {
	t.Parallel()
	c, db := newPreloadTestCQRS(t)
	ctx := context.Background()
	seedPreloadAuthor(t, db, "a1", "Ada")
	seedPreloadAuthor(t, db, "a2", "Grace")

	res, err := c.CreateManyFormat(ctx, []preloadPost{
		{ID: "p1", Title: "One", AuthorID: "a1"},
		{ID: "p2", Title: "Two", AuthorID: "a2"},
	}, "Author")
	if err != nil {
		t.Fatalf("CreateMany returned error: %v", err)
	}
	if len(res) != 2 || res[0].AuthorName != "Ada" || res[1].AuthorName != "Grace" {
		t.Fatalf("expected both records to have their author preloaded, got %+v, %+v", res[0], res[1])
	}
}

func TestPreload_HappyPath_CreateManyUnknownRelationIsDroppedNotAnError(t *testing.T) {
	t.Parallel()
	c, db := newPreloadTestCQRS(t)
	seedPreloadAuthor(t, db, "a1", "Ada")

	res, err := c.CreateManyFormat(context.Background(), []preloadPost{{ID: "p1", Title: "One", AuthorID: "a1"}}, "NotARealRelation")
	if err != nil {
		t.Fatalf("expected the unknown relation to be dropped rather than error, got: %v", err)
	}
	if len(res) != 1 || res[0].AuthorName != "" {
		t.Fatalf("expected Author not to be loaded, got %+v", res)
	}
}

func TestPreload_HappyPath_WithTxVariantsLoadRelations(t *testing.T) {
	t.Parallel()
	c, db := newPreloadTestCQRS(t)
	ctx := context.Background()
	seedPreloadAuthor(t, db, "a1", "Ada")
	seedPreloadAuthor(t, db, "a2", "Grace")

	err := db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		res, err := c.CreateWithTxFormat(ctx, tx, preloadPost{ID: "p1", Title: "One", AuthorID: "a1"}, "Author")
		if err != nil {
			return err
		}
		if res.AuthorName != "Ada" {
			t.Errorf("CreateWithTx: expected AuthorName 'Ada', got %q", res.AuthorName)
		}

		many, err := c.CreateManyWithTxFormat(ctx, tx, []preloadPost{{ID: "p2", Title: "Two", AuthorID: "a2"}}, "Author")
		if err != nil {
			return err
		}
		if len(many) != 1 || many[0].AuthorName != "Grace" {
			t.Errorf("CreateManyWithTx: expected AuthorName 'Grace', got %+v", many)
		}

		updated, err := c.UpdateByIDWithTxFormat(ctx, tx, "p1", preloadPost{ID: "p1", Title: "One Updated", AuthorID: "a2"}, "Author")
		if err != nil {
			return err
		}
		if updated.AuthorName != "Grace" {
			t.Errorf("UpdateByIDWithTx: expected AuthorName 'Grace', got %q", updated.AuthorName)
		}

		bulkUpdated, err := c.UpdateManyWithTxFormat(ctx, tx, []preloadPost{{ID: "p2", Title: "Two Updated", AuthorID: "a1"}}, "Author")
		if err != nil {
			return err
		}
		if len(bulkUpdated) != 1 || bulkUpdated[0].AuthorName != "Ada" {
			t.Errorf("UpdateManyWithTx: expected AuthorName 'Ada', got %+v", bulkUpdated)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("transaction failed: %v", err)
	}
}

func TestPreload_HappyPath_WithTxVariantsUnknownRelationIsDroppedNotAnError(t *testing.T) {
	t.Parallel()
	c, db := newPreloadTestCQRS(t)
	seedPreloadAuthor(t, db, "a1", "Ada")

	err := db.RunInTx(context.Background(), nil, func(ctx context.Context, tx bun.Tx) error {
		one, err := c.CreateWithTxFormat(ctx, tx, preloadPost{ID: "p1", Title: "One", AuthorID: "a1"}, "NotARealRelation")
		if err != nil {
			t.Errorf("CreateWithTx: expected the unknown relation to be dropped rather than error, got: %v", err)
		} else if one.AuthorName != "" {
			t.Errorf("CreateWithTx: expected Author not to be loaded, got %q", one.AuthorName)
		}
		many, err := c.CreateManyWithTxFormat(ctx, tx, []preloadPost{{ID: "p2", Title: "Two", AuthorID: "a1"}}, "NotARealRelation")
		if err != nil {
			t.Errorf("CreateManyWithTx: expected the unknown relation to be dropped rather than error, got: %v", err)
		} else if len(many) != 1 || many[0].AuthorName != "" {
			t.Errorf("CreateManyWithTx: expected Author not to be loaded, got %+v", many)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("transaction failed: %v", err)
	}
}

func TestPreload_HappyPath_UpdateManyLoadsRelationForEveryRecord(t *testing.T) {
	t.Parallel()
	c, db := newPreloadTestCQRS(t)
	ctx := context.Background()
	seedPreloadAuthor(t, db, "a1", "Ada")
	seedPreloadAuthor(t, db, "a2", "Grace")
	if _, err := c.CreateManyFormat(ctx, []preloadPost{
		{ID: "p1", Title: "One", AuthorID: "a1"},
		{ID: "p2", Title: "Two", AuthorID: "a1"},
	}); err != nil {
		t.Fatalf("seed CreateMany returned error: %v", err)
	}

	res, err := c.UpdateManyFormat(ctx, []preloadPost{
		{ID: "p1", Title: "One Updated", AuthorID: "a2"},
		{ID: "p2", Title: "Two Updated", AuthorID: "a2"},
	}, "Author")
	if err != nil {
		t.Fatalf("UpdateMany returned error: %v", err)
	}
	if len(res) != 2 || res[0].AuthorName != "Grace" || res[1].AuthorName != "Grace" {
		t.Fatalf("expected both updated records to have their new author preloaded, got %+v, %+v", res[0], res[1])
	}
}

func TestPreload_HappyPath_UpdateWithTxVariantsUnknownRelationIsDroppedNotAnError(t *testing.T) {
	t.Parallel()
	c, db := newPreloadTestCQRS(t)
	ctx := context.Background()
	seedPreloadAuthor(t, db, "a1", "Ada")
	if _, err := c.CreateFormat(ctx, preloadPost{ID: "p1", Title: "One", AuthorID: "a1"}); err != nil {
		t.Fatalf("seed Create returned error: %v", err)
	}

	err := db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		one, err := c.UpdateByIDWithTxFormat(ctx, tx, "p1", preloadPost{ID: "p1", Title: "Updated", AuthorID: "a1"}, "NotARealRelation")
		if err != nil {
			t.Errorf("UpdateByIDWithTx: expected the unknown relation to be dropped rather than error, got: %v", err)
		} else if one.AuthorName != "" {
			t.Errorf("UpdateByIDWithTx: expected Author not to be loaded, got %q", one.AuthorName)
		}
		many, err := c.UpdateManyWithTxFormat(ctx, tx, []preloadPost{{ID: "p1", Title: "Updated", AuthorID: "a1"}}, "NotARealRelation")
		if err != nil {
			t.Errorf("UpdateManyWithTx: expected the unknown relation to be dropped rather than error, got: %v", err)
		} else if len(many) != 1 || many[0].AuthorName != "" {
			t.Errorf("UpdateManyWithTx: expected Author not to be loaded, got %+v", many)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("transaction failed: %v", err)
	}
}

func TestPreload_HappyPath_UpdateByIDAndUpdateManyUnknownRelationIsDroppedNotAnError(t *testing.T) {
	t.Parallel()
	c, db := newPreloadTestCQRS(t)
	ctx := context.Background()
	seedPreloadAuthor(t, db, "a1", "Ada")
	if _, err := c.CreateFormat(ctx, preloadPost{ID: "p1", Title: "One", AuthorID: "a1"}); err != nil {
		t.Fatalf("seed Create returned error: %v", err)
	}

	one, err := c.UpdateByIDFormat(ctx, "p1", preloadPost{ID: "p1", Title: "Updated", AuthorID: "a1"}, "NotARealRelation")
	if err != nil {
		t.Errorf("UpdateByID: expected the unknown relation to be dropped rather than error, got: %v", err)
	} else if one.AuthorName != "" {
		t.Errorf("UpdateByID: expected Author not to be loaded, got %q", one.AuthorName)
	}
	many, err := c.UpdateManyFormat(ctx, []preloadPost{{ID: "p1", Title: "Updated", AuthorID: "a1"}}, "NotARealRelation")
	if err != nil {
		t.Errorf("UpdateMany: expected the unknown relation to be dropped rather than error, got: %v", err)
	} else if len(many) != 1 || many[0].AuthorName != "" {
		t.Errorf("UpdateMany: expected Author not to be loaded, got %+v", many)
	}
}
