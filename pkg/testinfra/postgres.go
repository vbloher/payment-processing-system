package testinfra

import (
	"fmt"
	"net"
	"net/url"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"github.com/vbloher/payment-processing-system/pkg/postgres"
)

const (
	PostgresImage = "postgres:18-alpine"

	pgUser     = "test"
	pgPassword = "test"
	pgAdminDB  = "postgres"
)

type Postgres struct{ Host, Port, User, Password string }

// StartPostgres starts one Postgres container with the given databases created.
// The container ends with the test.
func StartPostgres(t testing.TB, databases ...string) Postgres {
	t.Helper()

	ctr, err := tcpostgres.Run(t.Context(), PostgresImage,
		tcpostgres.WithDatabase(pgAdminDB),
		tcpostgres.WithUsername(pgUser),
		tcpostgres.WithPassword(pgPassword),
		tcpostgres.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(t, ctr)

	if err != nil {
		t.Fatalf("testinfra.StartPostgres: %v", err)
	}

	host, err := ctr.Host(t.Context())
	if err != nil {
		t.Fatalf("testinfra.StartPostgres: host: %v", err)
	}

	port, err := ctr.MappedPort(t.Context(), "5432/tcp")
	if err != nil {
		t.Fatalf("testinfra.StartPostgres: port: %v", err)
	}

	pg := Postgres{Host: host, Port: port.Port(), User: pgUser, Password: pgPassword}

	conn, err := pgx.Connect(t.Context(), pg.URL(pgAdminDB))
	if err != nil {
		t.Fatalf("testinfra.StartPostgres: connect: %v", err)
	}
	defer func() { _ = conn.Close(t.Context()) }()

	for _, name := range databases {
		if _, err = conn.Exec(t.Context(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
			t.Fatalf("testinfra.StartPostgres: create database %s: %v", name, err)
		}
	}

	return pg
}

func (p Postgres) Config(database string) postgres.Config {
	return postgres.Config{
		User:     p.User,
		Password: p.Password,
		Port:     p.Port,
		Host:     p.Host,
		DBName:   database,
	}
}

func (p Postgres) URL(database string) string {
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(p.User, p.Password),
		Host:     net.JoinHostPort(p.Host, p.Port),
		Path:     "/" + database,
		RawQuery: "sslmode=disable",
	}

	return u.String()
}

// Migrate applies every migration in migrationsDir to the database.
func Migrate(t testing.TB, databaseURL, migrationsDir string) {
	t.Helper()

	// The pgx/v5 driver registers the "pgx5" scheme.
	u, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatalf("testinfra.Migrate: parse url: %v", err)
	}

	u.Scheme = "pgx5"

	m, err := migrate.New("file://"+migrationsDir, u.String())
	if err != nil {
		t.Fatalf("testinfra.Migrate: %v", err)
	}

	defer func() {
		srcErr, dbErr := m.Close()
		if srcErr != nil || dbErr != nil {
			t.Logf("testinfra.Migrate: close: %s", fmt.Sprint(srcErr, dbErr))
		}
	}()

	if err = m.Up(); err != nil && err != migrate.ErrNoChange { //nolint:errorlint // sentinel from the library
		t.Fatalf("testinfra.Migrate: up: %v", err)
	}
}
