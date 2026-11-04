// Package deps pins the third-party modules the agents build against, so
// go.mod and go.sum don't change while three agents share one tree. It is
// deleted during integration once real imports exist.
package deps

import (
	_ "github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/prometheus/client_golang/prometheus"
	_ "github.com/prometheus/client_golang/prometheus/promhttp"
)
