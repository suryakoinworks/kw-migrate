# Koinworks Migration Tool (KMT)

Manage postgresql cluster migration easly

## Requirements

- Postgresql 9.5 or above

- Go 1.16 or above

- `pg_dump` (optional) to support reverse migration

## Features

- Support multiple connections and schemas

- Reverse migration from existing database

## Install

- Download latest release `https://github.com/suryakoinworks/kmt/tags`

- Extract source

- Download dependencies `cd kmt && go get && go mod tidy`

- Build `go build -o kmt`

- Run `mv kmt /usr/local/bin/kmt`

- Set executable `chmod a+x /usr/local/bin/kmt`

- Check using `kmt --help`

## Commands available

- `kmt create <schema> <name>` to create new migration file

- `kmt up <db> <schema>` to deploy migration(s) from database and schema which you provide

- `kmt down <db> <schema>` to drop migration(s) from database and schema which you provide

- `kmt generate <schema>` to reverse migration from your `source` database, this command use `PGPASSWORD` environment variable

- `kmt rollback <db> <schema> <step>` to rollback migration version from database and schema which you provide

- `kmt run <db> <schema> <step>` to run migration version from database and schema which you provide

- `kmt sync <cluster> <schema>` to sync migration in cluster for schema which you provide

Run `kmt --help` for complete commands

## Usage

- Create new project folder

- Copy Kwfile.yml below

```yaml
version: 1.0

migration:
    pg_dump: /usr/bin/pg_dump
    folder: migrations
    source: default
    clusters:
        local: [local]
    connections:
        default:
            host: default
            port: 5432
            name: database
            user: user
            password: s3cret
        local:
            host: localhost
            port: 5432
            name: database
            user: user
            password: s3cret
    schemas:
        public:
            excludes:
                - exclude_tables
            with_data:
                - data_included_tables
        user:
            excludes:
                - exclude_tables
            with_data:
                - data_included_tables
```

- Create new migration or generate from `source`

## TODO

- [x] Migrate tables
- [x] Migrate enums (UDT)
- [x] Migrate functions
- [x] Migrate views
- [x] Migrate materialized views
- [x] Show migration version
- [x] Show State/Compare
- [x] Upgrade Command
- [x] Refactor Codes
