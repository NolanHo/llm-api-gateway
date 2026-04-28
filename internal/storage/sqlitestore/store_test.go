package sqlitestore

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSelectLeastActiveAccountIgnoresInactiveSessions(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, filepath.Join(t.TempDir(), "test.sqlite3"), 30*time.Minute, 14*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(ctx)
	now := time.Now().UTC()
	accounts := []Account{
		{AccountID: "acc_a", ProviderKind: "copilot-api", DisplayName: "A", DownstreamHost: "127.0.0.1", DownstreamPort: 40000, Enabled: true, State: "running"},
		{AccountID: "acc_b", ProviderKind: "copilot-api", DisplayName: "B", DownstreamHost: "127.0.0.1", DownstreamPort: 40001, Enabled: true, State: "running"},
	}
	if err := store.UpsertAccounts(ctx, accounts, now); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertLineageBinding(ctx, "lin_active", accounts[0], "turn_1", now); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertLineageBinding(ctx, "lin_old", accounts[1], "turn_2", now.Add(-40*time.Minute)); err != nil {
		t.Fatal(err)
	}
	picked, err := store.SelectLeastActiveAccount(ctx, "", now)
	if err != nil {
		t.Fatal(err)
	}
	if picked.AccountID != "acc_b" {
		t.Fatalf("expected acc_b because old lineage should be inactive, got %s", picked.AccountID)
	}
}
