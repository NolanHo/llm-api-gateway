package app

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/nolanho/llm-api-gateway/internal/config"
	"github.com/nolanho/llm-api-gateway/internal/logging"
	"github.com/nolanho/llm-api-gateway/internal/storage/duckstore"
	"github.com/nolanho/llm-api-gateway/internal/storage/sqlitestore"
	"go.uber.org/zap"
)

type App struct {
	cfg    config.Config
	logger *zap.Logger
	sqlite *sqlitestore.Store
	duck   *duckstore.Store
	mux    *http.ServeMux
}

func New(ctx context.Context, cfg config.Config, logger *zap.Logger) (*App, error) {
	if err := os.MkdirAll(filepath.Dir(cfg.SQLitePath), 0o755); err != nil {
		return nil, fmt.Errorf("create sqlite dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(cfg.DuckDBPath), 0o755); err != nil {
		return nil, fmt.Errorf("create duckdb dir: %w", err)
	}

	sqliteStore, err := sqlitestore.Open(ctx, cfg.SQLitePath, cfg.ActiveSessionWindow, cfg.InactiveSessionRetain)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	duckStore, err := duckstore.Open(ctx, cfg.DuckDBPath)
	if err != nil {
		_ = sqliteStore.Close(ctx)
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	a := &App{
		cfg:    cfg,
		logger: logger,
		sqlite: sqliteStore,
		duck:   duckStore,
		mux:    http.NewServeMux(),
	}
	a.routes()
	logger.Info("storage initialized",
		logging.String("sqlite_path", cfg.SQLitePath),
		logging.String("duckdb_path", cfg.DuckDBPath),
		logging.Int64("active_session_window_ms", cfg.ActiveSessionWindow.Milliseconds()),
		logging.Int64("inactive_session_retention_ms", cfg.InactiveSessionRetain.Milliseconds()),
	)
	return a, nil
}

func (a *App) routes() {
	a.mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	a.mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := a.sqlite.Ping(ctx); err != nil {
			http.Error(w, fmt.Sprintf("sqlite not ready: %v", err), http.StatusServiceUnavailable)
			return
		}
		if err := a.duck.Ping(ctx); err != nil {
			http.Error(w, fmt.Sprintf("duckdb not ready: %v", err), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})
}

func (a *App) Handler() http.Handler { return a.mux }

func (a *App) Close(ctx context.Context) error {
	if err := a.duck.Close(ctx); err != nil {
		return err
	}
	return a.sqlite.Close(ctx)
}
