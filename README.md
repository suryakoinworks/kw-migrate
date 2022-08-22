# KW-Migrate

Manage postgresql cluster migration easly

## Requirements

- Go 1.17 or above

- `pg_dump` (optional) to support reverse migration

## Features

- Support multiple connections and schemas

- Reverse migration from existing database

## Install

- download latest release `https://github.com/suryakoinworks/kw-migrate/tags`

- extract source

- download dependencies `cd kw-migrate && go get && go mod tidy`

- build `go build -o kw-migrate`

- move to bin or add to environment variables

- check using `kw-migrate --help`

## Usage

- create `Kwfile.yml` see [example](https://github.com/suryakoinworks/kw-migrate/blob/main/Kwfile.example.yml)

- run `kw-migrate create <name>` to create new migration file

- run `kw-migrate up <db> <schema>` to deploy migration(s) to database and schema which you defined

- run `kw-migrate down <db> <schema>` to drop migration(s) to database and schema which you defined

- run `kw-migrate generate` to reverse migration from your `source` database 
