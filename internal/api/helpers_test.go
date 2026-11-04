package api_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

func dropSchema(t *testing.T, dsn, schema string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Logf("drop schema: %v", err)
		return
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
		t.Logf("drop schema: %v", err)
	}
}
