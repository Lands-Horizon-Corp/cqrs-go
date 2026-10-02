package regression

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/pagination"
)

// These tests model MemberProfileQuickSearch / AccountQuickSearch as ModeCustom filters.
// SQLite's LIKE stands in for Postgres ILIKE, and similarity ordering becomes a plain sort.

type qsMedia struct {
	bun.BaseModel `bun:"table:qs_media"`

	ID       string `bun:"id,pk"`
	FileName string `bun:"file_name"`
}

type qsMember struct {
	bun.BaseModel `bun:"table:qs_members"`

	ID             string   `bun:"id,pk"`
	OrganizationID string   `bun:"organization_id"`
	BranchID       string   `bun:"branch_id"`
	FullName       string   `bun:"full_name"`
	FirstName      string   `bun:"first_name"`
	LastName       string   `bun:"last_name"`
	OldReferenceID *string  `bun:"old_reference_id"`
	Passbook       *string  `bun:"passbook"`
	MediaID        *string  `bun:"media_id"`
	Media          *qsMedia `bun:"rel:belongs-to,join:media_id=id"`
}

type qsAccount struct {
	bun.BaseModel `bun:"table:qs_accounts"`

	ID             string  `bun:"id,pk"`
	OrganizationID string  `bun:"organization_id"`
	BranchID       string  `bun:"branch_id"`
	Name           string  `bun:"name"`
	Description    *string `bun:"description"`
}

const (
	qsTotalMembers   = 11
	qsOrg1Members    = 10
	qsTotalAccounts  = 5
	qsMaxSearchBytes = 200
)

func qsEscapeLike(word string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(word)
}

func qsSearchTerm(name string, value any) (string, error) {
	search, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%s: expected a string, got %T", name, value)
	}
	if len(search) > qsMaxSearchBytes {
		return "", fmt.Errorf("%s: search is too long", name)
	}
	// Drivers disagree on NUL handling, so it is rejected before reaching SQL.
	if strings.ContainsRune(search, 0) {
		return "", fmt.Errorf("%s: search contains a NUL byte", name)
	}
	return search, nil
}

func memberQuickSearch(q *bun.SelectQuery, value any) (*bun.SelectQuery, error) {
	search, err := qsSearchTerm("memberQuickSearch", value)
	if err != nil {
		return nil, err
	}
	for word := range strings.FieldsSeq(search) {
		pattern := "%" + qsEscapeLike(word) + "%"
		q = q.WhereGroup("AND", func(g *bun.SelectQuery) *bun.SelectQuery {
			return g.Where(`full_name LIKE ? ESCAPE '\'`, pattern).
				WhereOr(`old_reference_id LIKE ? ESCAPE '\'`, pattern).
				WhereOr(`passbook LIKE ? ESCAPE '\'`, pattern).
				WhereOr(`first_name LIKE ? ESCAPE '\'`, pattern).
				WhereOr(`last_name LIKE ? ESCAPE '\'`, pattern)
		})
	}
	return q, nil
}

func accountQuickSearch(q *bun.SelectQuery, value any) (*bun.SelectQuery, error) {
	search, err := qsSearchTerm("accountQuickSearch", value)
	if err != nil {
		return nil, err
	}
	for word := range strings.FieldsSeq(search) {
		pattern := "%" + qsEscapeLike(word) + "%"
		q = q.WhereGroup("AND", func(g *bun.SelectQuery) *bun.SelectQuery {
			return g.Where(`name LIKE ? ESCAPE '\'`, pattern).
				WhereOr(`description LIKE ? ESCAPE '\'`, pattern)
		})
	}
	return q, nil
}

// qsScope mirrors the optional organization/branch/search checks of the quick-search functions.
func qsScope(name string, fn domains.CustomFilter, orgID, branchID *string, search string) domains.StructuredFilter {
	scope := domains.StructuredFilter{Logic: domains.LogicAnd}
	if orgID != nil {
		scope.Filters = append(scope.Filters, domains.Filter{
			Field: "organization_id", Mode: domains.ModeEqual, Value: *orgID,
		})
	}
	if branchID != nil {
		scope.Filters = append(scope.Filters, domains.Filter{
			Field: "branch_id", Mode: domains.ModeEqual, Value: *branchID,
		})
	}
	if search != "" {
		scope.Filters = append(scope.Filters, domains.Filter{
			Field: name, Mode: domains.ModeCustom, Value: search, Custom: fn,
		})
	}
	return scope
}

func memberScope(orgID, branchID *string, search string) domains.StructuredFilter {
	return qsScope("memberQuickSearch", memberQuickSearch, orgID, branchID, search)
}

func accountScope(orgID, branchID *string, search string) domains.StructuredFilter {
	return qsScope("accountQuickSearch", accountQuickSearch, orgID, branchID, search)
}

func newQuickSearchDB(t *testing.T) *fakeSQLService {
	t.Helper()
	db := newFakeSQLService(t)
	ctx := context.Background()
	for _, model := range []any{(*qsMedia)(nil), (*qsMember)(nil), (*qsAccount)(nil)} {
		if _, err := db.Client().NewCreateTable().Model(model).Exec(ctx); err != nil {
			t.Fatalf("creating quick-search table: %v", err)
		}
	}

	media := []qsMedia{
		{ID: "med1", FileName: "photo1.png"},
		{ID: "med2", FileName: "photo2.png"},
	}
	members := []qsMember{
		{
			ID: "m1", OrganizationID: "org-1", BranchID: "br-1", FullName: "Ann Marie Lee",
			FirstName: "Ann", LastName: "Lee", OldReferenceID: new("OLD-001"), Passbook: new("PB-100"),
			MediaID: new("med1"),
		},
		{
			ID: "m2", OrganizationID: "org-1", BranchID: "br-1", FullName: "Ann Cole",
			FirstName: "Ann", LastName: "Cole", Passbook: new("PB-200"),
		},
		{
			ID: "m3", OrganizationID: "org-1", BranchID: "br-2", FullName: "Bob Lee",
			FirstName: "Bob", LastName: "Lee", OldReferenceID: new("OLD-300"), MediaID: new("med2"),
		},
		{
			ID: "m4", OrganizationID: "org-2", BranchID: "br-1", FullName: "Ann Lee",
			FirstName: "Ann", LastName: "Lee", OldReferenceID: new("OLD-400"), Passbook: new("PB-400"),
		},
		{
			ID: "m5", OrganizationID: "org-1", BranchID: "br-1", FullName: "Zed Quill",
			FirstName: "Zed", LastName: "Quill", OldReferenceID: new("REF-500"), Passbook: new("50%_off"),
		},
		// Each probe member matches a search term through exactly one column.
		{ID: "p1", OrganizationID: "org-1", BranchID: "br-1", FullName: "Quentin Tarantino", FirstName: "Q", LastName: "T"},
		{ID: "p2", OrganizationID: "org-1", BranchID: "br-1", FullName: "H. Von", FirstName: "Hildegard", LastName: "Von"},
		{ID: "p3", OrganizationID: "org-1", BranchID: "br-1", FullName: "F. Hubert", FirstName: "F.", LastName: "Featherstone"},
		{
			ID: "p4", OrganizationID: "org-1", BranchID: "br-1", FullName: "Probe Four",
			FirstName: "Pro", LastName: "Four", OldReferenceID: new("LEGACY-9"),
		},
		{
			ID: "p5", OrganizationID: "org-1", BranchID: "br-1", FullName: "Probe Five",
			FirstName: "Pro", LastName: "Five", Passbook: new("BOOK-42"),
		},
		{ID: "u1", OrganizationID: "org-1", BranchID: "br-1", FullName: "José Núñez", FirstName: "José", LastName: "Núñez"},
	}
	accounts := []qsAccount{
		{ID: "a1", OrganizationID: "org-1", BranchID: "br-1", Name: "Cash on Hand", Description: new("Petty cash drawer")},
		{ID: "a2", OrganizationID: "org-1", BranchID: "br-1", Name: "Savings Account"},
		{ID: "a3", OrganizationID: "org-1", BranchID: "br-1", Name: "Cash Equivalents", Description: new("Short term")},
		{
			ID: "a4", OrganizationID: "org-1", BranchID: "br-2", Name: "Loans Receivable",
			Description: new("Cash advances to members"),
		},
		{ID: "a5", OrganizationID: "org-2", BranchID: "br-1", Name: "Cash Other Org"},
	}
	for _, rows := range []any{&media, &members, &accounts} {
		if _, err := db.Client().NewInsert().Model(rows).Exec(ctx); err != nil {
			t.Fatalf("seeding quick-search rows: %v", err)
		}
	}
	return db
}

func newQSMemberService(db *fakeSQLService) *pagination.PaginationService[qsMember, string] {
	return pagination.NewPaginationService(pagination.PaginationService[qsMember, string]{
		ReadSQLService:    db,
		ColumnDefaultSort: "id ASC",
	})
}

func newQSAccountService(db *fakeSQLService) *pagination.PaginationService[qsAccount, string] {
	return pagination.NewPaginationService(pagination.PaginationService[qsAccount, string]{
		ReadSQLService:    db,
		ColumnDefaultSort: "id ASC",
	})
}

func qsMemberIDs(data []*qsMember) []string {
	ids := make([]string, 0, len(data))
	for _, m := range data {
		ids = append(ids, m.ID)
	}
	return ids
}

func qsAccountIDs(data []*qsAccount) []string {
	ids := make([]string, 0, len(data))
	for _, a := range data {
		ids = append(ids, a.ID)
	}
	return ids
}

func qsAssertSet(t *testing.T, label string, got []string, want ...string) {
	t.Helper()
	got = slices.Sorted(slices.Values(got))
	want = slices.Sorted(slices.Values(want))
	if !slices.Equal(got, want) {
		t.Fatalf("%s: expected %v, got %v", label, want, got)
	}
}

func qsFindMembers(t *testing.T, svc *pagination.PaginationService[qsMember, string], scope domains.StructuredFilter) []string {
	t.Helper()
	data, err := svc.Find(context.Background(), scope)
	if err != nil {
		t.Fatalf("Find returned error: %v", err)
	}
	return qsMemberIDs(data)
}

func qsParse(t *testing.T, filter domains.StructuredFilter) domains.Pagination {
	t.Helper()
	reqCtx := newPaginationTestContext(t, map[string]string{
		"filter":   encodeQueryParam(t, filter),
		"pageSize": "50",
	})
	var p domains.Pagination
	if err := p.Parse(reqCtx); err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	return p
}

func qsAssertTablesIntact(t *testing.T, db *fakeSQLService) {
	t.Helper()
	members, err := newQSMemberService(db).Count(context.Background(), domains.StructuredFilter{})
	if err != nil || members != qsTotalMembers {
		t.Fatalf("expected %d members after the search, got %d (err %v)", qsTotalMembers, members, err)
	}
	accounts, err := newQSAccountService(db).Count(context.Background(), domains.StructuredFilter{})
	if err != nil || accounts != qsTotalAccounts {
		t.Fatalf("expected %d accounts after the search, got %d (err %v)", qsTotalAccounts, accounts, err)
	}
}

// --- happy path: member quick search ---

func TestQuickSearchMember_HappyPath_EachSearchedColumnMatchesOnItsOwn(t *testing.T) {
	t.Parallel()
	svc := newQSMemberService(newQuickSearchDB(t))

	tests := []struct {
		name   string
		search string
		want   string
	}{
		{"full name", "tarantino", "p1"},
		{"first name", "hildegard", "p2"},
		{"last name", "featherstone", "p3"},
		{"old reference id", "legacy-9", "p4"},
		{"passbook", "book-42", "p5"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := qsFindMembers(t, svc, memberScope(new("org-1"), nil, tc.search))
			qsAssertSet(t, tc.search, got, tc.want)
		})
	}
}

func TestQuickSearchMember_HappyPath_EveryWordMustMatchSomeColumn(t *testing.T) {
	t.Parallel()
	svc := newQSMemberService(newQuickSearchDB(t))
	org := new("org-1")

	tests := []struct {
		name   string
		search string
		want   []string
	}{
		{"one word", "ann", []string{"m1", "m2"}},
		{"words matching the same column", "ann lee", []string{"m1"}},
		{"word order does not matter", "lee ann", []string{"m1"}},
		{"words matching different columns", "ann pb-100", []string{"m1"}},
		{"extra spaces between words", "  ann    lee  ", []string{"m1"}},
		{"one word matches nothing", "ann nobody", nil},
		{"case is ignored", "ANN lEe", []string{"m1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			qsAssertSet(t, tc.search, qsFindMembers(t, svc, memberScope(org, nil, tc.search)), tc.want...)
		})
	}
}

func TestQuickSearchMember_HappyPath_ScopedByOrganizationAndBranch(t *testing.T) {
	t.Parallel()
	svc := newQSMemberService(newQuickSearchDB(t))

	qsAssertSet(t, "any organization", qsFindMembers(t, svc, memberScope(nil, nil, "ann")), "m1", "m2", "m4")
	qsAssertSet(t, "org-1", qsFindMembers(t, svc, memberScope(new("org-1"), nil, "ann")), "m1", "m2")
	qsAssertSet(t, "org-2", qsFindMembers(t, svc, memberScope(new("org-2"), nil, "ann")), "m4")
	qsAssertSet(t, "org-1 br-1", qsFindMembers(t, svc, memberScope(new("org-1"), new("br-1"), "lee")), "m1")
	qsAssertSet(t, "org-1 br-2", qsFindMembers(t, svc, memberScope(new("org-1"), new("br-2"), "lee")), "m3")
	qsAssertSet(t, "org-1 br-2 ann", qsFindMembers(t, svc, memberScope(new("org-1"), new("br-2"), "ann")))
}

func TestQuickSearchMember_HappyPath_PreloadsMediaOnlyWhenPresent(t *testing.T) {
	t.Parallel()
	svc := newQSMemberService(newQuickSearchDB(t))

	got, err := svc.Find(context.Background(), memberScope(new("org-1"), nil, "ann"), "Media")
	if err != nil {
		t.Fatalf("Find returned error: %v", err)
	}
	byID := map[string]*qsMember{}
	for _, m := range got {
		byID[m.ID] = m
	}
	if len(byID) != 2 {
		t.Fatalf("expected m1 and m2, got %v", qsMemberIDs(got))
	}
	if byID["m1"].Media == nil || byID["m1"].Media.FileName != "photo1.png" {
		t.Fatalf("expected m1's media to be loaded, got %+v", byID["m1"].Media)
	}
	if byID["m2"].Media != nil {
		t.Fatalf("expected m2 (no media) to have a nil Media, got %+v", byID["m2"].Media)
	}
}

func TestQuickSearchMember_HappyPath_UnknownPreloadIsDroppedNotAnError(t *testing.T) {
	t.Parallel()
	svc := newQSMemberService(newQuickSearchDB(t))

	got, err := svc.Find(context.Background(), memberScope(new("org-1"), nil, "ann"), "NotARelation")
	if err != nil {
		t.Fatalf("Find returned error: %v", err)
	}
	qsAssertSet(t, "unknown preload", qsMemberIDs(got), "m1", "m2")
}

func TestQuickSearchMember_HappyPath_FifteenResultLimitThenCursorWalksTheRest(t *testing.T) {
	t.Parallel()
	db := newQuickSearchDB(t)
	svc := newQSMemberService(db)
	ctx := context.Background()

	bulk := make([]qsMember, 20)
	for i := range bulk {
		bulk[i] = qsMember{
			ID: fmt.Sprintf("lim%02d", i), OrganizationID: "org-9", BranchID: "br-1",
			FullName: fmt.Sprintf("Limit Person %02d", i), FirstName: "Limit", LastName: "Person",
		}
	}
	if _, err := db.Client().NewInsert().Model(&bulk).Exec(ctx); err != nil {
		t.Fatalf("seeding bulk members: %v", err)
	}

	scope := memberScope(new("org-9"), nil, "limit person")
	sorted := domains.StructuredFilter{SortFields: []domains.SortField{{Field: "full_name", Order: domains.SortOrderAsc}}}

	first, err := svc.PaginateFilter(ctx, scope, domains.Pagination{Filter: sorted, PageSize: 15})
	if err != nil {
		t.Fatalf("first page returned error: %v", err)
	}
	if len(first.Data) != 15 || first.NextCursor == nil {
		t.Fatalf("expected 15 rows and a next cursor, got %d / %v", len(first.Data), first.NextCursor)
	}

	second, err := svc.PaginateFilter(ctx, scope, domains.Pagination{Filter: sorted, PageSize: 15, Cursor: first.NextCursor})
	if err != nil {
		t.Fatalf("second page returned error: %v", err)
	}
	if len(second.Data) != 5 || second.NextCursor != nil {
		t.Fatalf("expected the remaining 5 rows and no next cursor, got %d / %v", len(second.Data), second.NextCursor)
	}
	if first.Data[14].FullName != "Limit Person 14" || second.Data[0].FullName != "Limit Person 15" {
		t.Fatalf("expected the pages to continue in order, got %q then %q", first.Data[14].FullName, second.Data[0].FullName)
	}
}

func TestQuickSearchMember_HappyPath_MixedDirectionCursorKeepsFilterOnEveryPage(t *testing.T) {
	t.Parallel()
	db := newQuickSearchDB(t)
	svc := newQSMemberService(db)
	ctx := context.Background()

	rows := []qsMember{
		{ID: "mx1", FullName: "Mix Alpha", FirstName: "f1"},
		{ID: "mx2", FullName: "Mix Alpha", FirstName: "f2"},
		{ID: "mx3", FullName: "Mix Beta", FirstName: "f3"},
		{ID: "mx4", FullName: "Mix Beta", FirstName: "f4"},
		{ID: "mx5", FullName: "Mix Beta", FirstName: "f5"},
		{ID: "mx6", FullName: "Mix Gamma", FirstName: "f6"},
		{ID: "mx7", FullName: "Mix Gamma", FirstName: "f7"},
		{ID: "noise1", FullName: "Other Person", FirstName: "f8"},
		{ID: "noise2", FullName: "Mix Elsewhere", FirstName: "f9", OrganizationID: "org-other"},
	}
	for i := range rows {
		if rows[i].OrganizationID == "" {
			rows[i].OrganizationID = "org-5"
		}
		rows[i].BranchID = "br-1"
	}
	if _, err := db.Client().NewInsert().Model(&rows).Exec(ctx); err != nil {
		t.Fatalf("seeding mixed-direction members: %v", err)
	}

	scope := memberScope(new("org-5"), nil, "mix")
	// Ascending name with descending first name cannot be a single row comparison.
	sorted := domains.StructuredFilter{SortFields: []domains.SortField{
		{Field: "full_name", Order: domains.SortOrderAsc},
		{Field: "first_name", Order: domains.SortOrderDesc},
	}}

	var order []string
	var cursor *string
	for range 10 {
		page, err := svc.PaginateFilter(ctx, scope, domains.Pagination{Filter: sorted, PageSize: 2, Cursor: cursor})
		if err != nil {
			t.Fatalf("PaginateFilter returned error: %v", err)
		}
		order = append(order, qsMemberIDs(page.Data)...)
		if page.NextCursor == nil {
			break
		}
		cursor = page.NextCursor
	}
	want := []string{"mx2", "mx1", "mx5", "mx4", "mx3", "mx7", "mx6"}
	if !slices.Equal(order, want) {
		t.Fatalf("expected %v across pages, got %v", want, order)
	}
}

func TestQuickSearchMember_HappyPath_CountExistsFindOneAndFindWithTxHonorTheFilter(t *testing.T) {
	t.Parallel()
	db := newQuickSearchDB(t)
	svc := newQSMemberService(db)
	ctx := context.Background()
	org := new("org-1")

	count, err := svc.Count(ctx, memberScope(org, nil, "ann"))
	if err != nil || count != 2 {
		t.Fatalf("expected Count 2, got %d (err %v)", count, err)
	}
	exists, err := svc.Exists(ctx, memberScope(org, nil, "ann"))
	if err != nil || !exists {
		t.Fatalf("expected Exists true, got %v (err %v)", exists, err)
	}
	exists, err = svc.Exists(ctx, memberScope(org, nil, "zzzz"))
	if err != nil || exists {
		t.Fatalf("expected Exists false, got %v (err %v)", exists, err)
	}

	one, err := svc.FindOne(ctx, memberScope(org, nil, "tarantino"))
	if err != nil || one.ID != "p1" {
		t.Fatalf("expected FindOne p1, got %+v (err %v)", one, err)
	}
	if _, err := svc.FindOne(ctx, memberScope(org, nil, "nobody")); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows for no match, got %v", err)
	}

	tx, err := db.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx returned error: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	inTx, err := svc.FindWithTx(ctx, &tx, memberScope(org, nil, "ann"))
	if err != nil {
		t.Fatalf("FindWithTx returned error: %v", err)
	}
	qsAssertSet(t, "FindWithTx", qsMemberIDs(inTx), "m1", "m2")
}

func TestQuickSearchMember_HappyPath_OrLogicBetweenTwoSearches(t *testing.T) {
	t.Parallel()
	svc := newQSMemberService(newQuickSearchDB(t))

	scope := domains.StructuredFilter{
		Logic: domains.LogicOr,
		Filters: []domains.Filter{
			{Field: "memberQuickSearch", Mode: domains.ModeCustom, Value: "tarantino", Custom: memberQuickSearch},
			{Field: "memberQuickSearch", Mode: domains.ModeCustom, Value: "hildegard", Custom: memberQuickSearch},
		},
	}
	qsAssertSet(t, "tarantino OR hildegard", qsFindMembers(t, svc, scope), "p1", "p2")
}

// --- happy path: account quick search ---

func TestQuickSearchAccount_HappyPath_MatchesNameOrNullableDescription(t *testing.T) {
	t.Parallel()
	svc := newQSAccountService(newQuickSearchDB(t))
	ctx := context.Background()
	org := new("org-1")

	tests := []struct {
		name   string
		search string
		want   []string
	}{
		{"name only match", "savings", []string{"a2"}},
		{"description only match", "advances", []string{"a4"}},
		{"name or description across branches", "cash", []string{"a1", "a3", "a4"}},
		{"words split across name and description", "short cash", []string{"a3"}},
		{"no match", "crypto", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := svc.Find(ctx, accountScope(org, nil, tc.search))
			if err != nil {
				t.Fatalf("Find returned error: %v", err)
			}
			qsAssertSet(t, tc.search, qsAccountIDs(got), tc.want...)
		})
	}
}

func TestQuickSearchAccount_HappyPath_BranchScopeAndNameOrdering(t *testing.T) {
	t.Parallel()
	svc := newQSAccountService(newQuickSearchDB(t))
	byName := domains.Pagination{
		Filter:   domains.StructuredFilter{SortFields: []domains.SortField{{Field: "name", Order: domains.SortOrderAsc}}},
		PageSize: 20,
	}

	page, err := svc.PaginateFilter(context.Background(), accountScope(new("org-1"), new("br-1"), "cash"), byName)
	if err != nil {
		t.Fatalf("PaginateFilter returned error: %v", err)
	}
	if got := qsAccountIDs(page.Data); !slices.Equal(got, []string{"a3", "a1"}) {
		t.Fatalf("expected [a3 a1] ordered by name, got %v", got)
	}

	all, err := svc.PaginateFilter(context.Background(), accountScope(new("org-1"), new("br-1"), ""), byName)
	if err != nil {
		t.Fatalf("PaginateFilter returned error: %v", err)
	}
	if got := qsAccountIDs(all.Data); !slices.Equal(got, []string{"a3", "a1", "a2"}) {
		t.Fatalf("expected an empty search to list the whole branch by name, got %v", got)
	}
}

// --- poison pills ---

func TestQuickSearchPoison_SQLInjectionStringsAreInertAndTablesSurvive(t *testing.T) {
	t.Parallel()
	db := newQuickSearchDB(t)
	members := newQSMemberService(db)
	accounts := newQSAccountService(db)
	ctx := context.Background()

	poison := []string{
		`';DROP/**/TABLE/**/qs_members;--`,
		`'||(SELECT/**/1)||'`,
		`') OR ('1'='1`,
		`" OR 1=1 --`,
		`x' UNION SELECT id FROM qs_accounts --`,
		`1; DELETE FROM qs_members`,
		`'`, `"`, `\`, `\'`, `--`, `/*`, `*/`,
		`?`, `??`, `\?`, `$1`, `:name`, `@p1`, `{{.}}`,
	}
	for _, input := range poison {
		t.Run(input, func(t *testing.T) {
			got, err := members.Find(ctx, memberScope(new("org-1"), nil, input))
			if err != nil {
				t.Fatalf("member search returned error: %v", err)
			}
			if len(got) != 0 {
				t.Fatalf("expected no member to match %q, got %v", input, qsMemberIDs(got))
			}
			accs, err := accounts.Find(ctx, accountScope(new("org-1"), nil, input))
			if err != nil {
				t.Fatalf("account search returned error: %v", err)
			}
			if len(accs) != 0 {
				t.Fatalf("expected no account to match %q, got %v", input, qsAccountIDs(accs))
			}
		})
	}
	qsAssertTablesIntact(t, db)
}

func TestQuickSearchPoison_LikeWildcardsAreMatchedLiterally(t *testing.T) {
	t.Parallel()
	svc := newQSMemberService(newQuickSearchDB(t))
	org := new("org-1")

	tests := []struct {
		name   string
		search string
		want   []string
	}{
		{"percent only matches a literal percent", "%", []string{"m5"}},
		{"underscore only matches a literal underscore", "_", []string{"m5"}},
		{"percent then underscore", "%_", []string{"m5"}},
		{"literal prefix before percent", "50%", []string{"m5"}},
		{"underscore is not a single-character wildcard", "5_", nil},
		{"double percent is not a wildcard run", "%%", nil},
		{"backslash is literal", `\`, nil},
		{"escaped percent is not an escape", `50\%`, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			qsAssertSet(t, tc.search, qsFindMembers(t, svc, memberScope(org, nil, tc.search)), tc.want...)
		})
	}
}

func TestQuickSearchPoison_EmptyAndWhitespaceSearchAddsNoCondition(t *testing.T) {
	t.Parallel()
	svc := newQSMemberService(newQuickSearchDB(t))
	org := new("org-1")

	for _, search := range []string{"", "   ", "\t\n \r\n"} {
		got := qsFindMembers(t, svc, memberScope(org, nil, search))
		if len(got) != qsOrg1Members {
			t.Fatalf("expected %q to list every org-1 member (%d), got %d", search, qsOrg1Members, len(got))
		}
	}
}

func TestQuickSearchPoison_WrongValueTypesReturnAnErrorNotAPanic(t *testing.T) {
	t.Parallel()
	svc := newQSMemberService(newQuickSearchDB(t))

	values := map[string]any{
		"nil":        nil,
		"int":        42,
		"float":      4.2,
		"bool":       true,
		"string ptr": new("ann"),
		"slice":      []string{"ann"},
		"map":        map[string]any{"q": "ann"},
		"struct":     struct{ Q string }{Q: "ann"},
	}
	for name, value := range values {
		t.Run(name, func(t *testing.T) {
			scope := domains.StructuredFilter{Filters: []domains.Filter{
				{Field: "memberQuickSearch", Mode: domains.ModeCustom, Value: value, Custom: memberQuickSearch},
			}}
			if _, err := svc.Find(context.Background(), scope); err == nil {
				t.Fatalf("expected an error for a %s value, got nil", name)
			}
		})
	}
}

func TestQuickSearchPoison_WrongValueTypesFromFrontendJSONAreDroppedNotPanics(t *testing.T) {
	t.Parallel()
	svc := newQSMemberService(newQuickSearchDB(t))

	for name, value := range map[string]any{"number": 42, "array": []string{"ann"}, "object": map[string]any{"q": "ann"}, "null": nil} {
		t.Run(name, func(t *testing.T) {
			p := qsParse(t, domains.StructuredFilter{Filters: []domains.Filter{
				{Field: "memberQuickSearch", Mode: domains.ModeCustom, Value: value},
			}})
			result, err := svc.Paginate(context.Background(), p)
			if err != nil {
				t.Fatalf("expected the client's custom filter to be dropped, got error: %v", err)
			}
			if len(result.Data) != qsTotalMembers {
				t.Fatalf("expected every member (%d) since the filter is dropped, got %v", qsTotalMembers, qsMemberIDs(result.Data))
			}
		})
	}
}

func TestQuickSearchPoison_OversizedAndNULSearchesAreRejectedBeforeTheQuery(t *testing.T) {
	t.Parallel()
	svc := newQSMemberService(newQuickSearchDB(t))
	org := new("org-1")

	rejected := map[string]string{
		"one byte over the limit": strings.Repeat("a", qsMaxSearchBytes+1),
		"huge":                    strings.Repeat("ann ", 50_000),
		"NUL byte":                "ann\x00",
		"only a NUL byte":         "\x00",
		"NUL between words":       "ann \x00 lee",
	}
	for name, search := range rejected {
		t.Run(name, func(t *testing.T) {
			if _, err := svc.Find(context.Background(), memberScope(org, nil, search)); err == nil {
				t.Fatalf("expected %s to be rejected, got nil error", name)
			}
		})
	}
}

func TestQuickSearchPoison_SearchAtTheLimitWithManyWordsStillRuns(t *testing.T) {
	t.Parallel()
	svc := newQSMemberService(newQuickSearchDB(t))

	search := strings.Repeat("a ", qsMaxSearchBytes/2)
	if len(search) != qsMaxSearchBytes {
		t.Fatalf("test setup: expected %d bytes, got %d", qsMaxSearchBytes, len(search))
	}
	got := qsFindMembers(t, svc, memberScope(new("org-1"), nil, search))
	if len(got) == 0 {
		t.Fatal("expected members containing the letter a to match")
	}
}

func TestQuickSearchPoison_UnicodeAndEmojiAreHandled(t *testing.T) {
	t.Parallel()
	svc := newQSMemberService(newQuickSearchDB(t))
	org := new("org-1")

	qsAssertSet(t, "accented", qsFindMembers(t, svc, memberScope(org, nil, "núñez")), "u1")
	qsAssertSet(t, "accented first name", qsFindMembers(t, svc, memberScope(org, nil, "josé")), "u1")
	qsAssertSet(t, "both words", qsFindMembers(t, svc, memberScope(org, nil, "josé núñez")), "u1")
	qsAssertSet(t, "emoji", qsFindMembers(t, svc, memberScope(org, nil, "😀")))
	qsAssertSet(t, "combining mark", qsFindMembers(t, svc, memberScope(org, nil, "e\u0301")))
	qsAssertSet(t, "right-to-left text", qsFindMembers(t, svc, memberScope(org, nil, "مرحبا")))
}

func TestQuickSearchPoison_FrontendOrLogicCannotEscapeTheBackendTenantScope(t *testing.T) {
	t.Parallel()
	svc := newQSMemberService(newQuickSearchDB(t))

	p := qsParse(t, domains.StructuredFilter{
		Logic: domains.LogicOr,
		Filters: []domains.Filter{
			{Field: "memberQuickSearch", Mode: domains.ModeCustom, Value: "ann"},
			{Field: "organization_id", Mode: domains.ModeEqual, Value: "org-2"},
			{Field: "id", Mode: domains.ModeIsNotEmpty},
		},
	})
	scope := memberScope(new("org-1"), nil, "")

	result, err := svc.PaginateFilter(context.Background(), scope, p)
	if err != nil {
		t.Fatalf("PaginateFilter returned error: %v", err)
	}
	for _, m := range result.Data {
		if m.OrganizationID != "org-1" {
			t.Fatalf("expected only org-1 rows, but %s belongs to %s", m.ID, m.OrganizationID)
		}
	}
	if len(result.Data) != qsOrg1Members {
		t.Fatalf("expected every org-1 member through the frontend's OR group, got %v", qsMemberIDs(result.Data))
	}
}

func TestQuickSearchPoison_FrontendCannotRunAnyCustomFilterByName(t *testing.T) {
	t.Parallel()
	db := newQuickSearchDB(t)
	svc := newQSMemberService(db)

	names := []string{
		"1=1; --",
		"full_name",
		"memberQuickSearch",
		"MEMBERQUICKSEARCH",
		"accountQuickSearch",
		"LOWER(full_name) <-> 'x'",
		"",
	}
	filters := make([]domains.Filter, 0, len(names))
	for _, name := range names {
		filters = append(filters, domains.Filter{Field: name, Mode: domains.ModeCustom, Value: "ann"})
	}
	p := qsParse(t, domains.StructuredFilter{Filters: filters})

	result, err := svc.Paginate(context.Background(), p)
	if err != nil {
		t.Fatalf("Paginate returned error: %v", err)
	}
	if len(result.Data) != qsTotalMembers {
		t.Fatalf("expected every client custom filter to be dropped (%d rows), got %v", qsTotalMembers, qsMemberIDs(result.Data))
	}
	qsAssertTablesIntact(t, db)
}

func TestQuickSearchPoison_ClientCustomFilterIsDroppedWhileBackendCustomFilterStillRuns(t *testing.T) {
	t.Parallel()
	svc := newQSMemberService(newQuickSearchDB(t))

	p := qsParse(t, domains.StructuredFilter{Filters: []domains.Filter{
		{Field: "memberQuickSearch", Mode: domains.ModeCustom, Value: "ann"},
	}})
	result, err := svc.PaginateFilter(context.Background(), memberScope(new("org-1"), nil, "tarantino"), p)
	if err != nil {
		t.Fatalf("PaginateFilter returned error: %v", err)
	}
	qsAssertSet(t, "backend search with a dropped client filter", qsMemberIDs(result.Data), "p1")
}

func TestQuickSearchPoison_CustomFuncReturningANilQueryIsAnErrorNotAPanic(t *testing.T) {
	t.Parallel()
	svc := newQSMemberService(newQuickSearchDB(t))

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("expected an error, but the filter panicked: %v", r)
		}
	}()
	scope := domains.StructuredFilter{Filters: []domains.Filter{
		{Mode: domains.ModeCustom, Custom: func(*bun.SelectQuery, any) (*bun.SelectQuery, error) { return nil, nil }},
	}}
	if _, err := svc.Find(context.Background(), scope); err == nil {
		t.Fatal("expected an error for a custom filter that returns a nil query, got nil")
	}
}

func TestQuickSearchPoison_FailingCustomFilterInsideOrGroupFailsTheWholeQuery(t *testing.T) {
	t.Parallel()
	svc := newQSMemberService(newQuickSearchDB(t))

	scope := domains.StructuredFilter{
		Logic: domains.LogicOr,
		Filters: []domains.Filter{
			{Field: "id", Mode: domains.ModeIsNotEmpty},
			{Mode: domains.ModeCustom, Custom: func(*bun.SelectQuery, any) (*bun.SelectQuery, error) {
				return nil, errors.New("boom")
			}},
		},
	}
	result, err := svc.Find(context.Background(), scope)
	if err == nil {
		t.Fatalf("expected the failing filter to fail the query, got %d rows", len(result))
	}
}
