package db

type (
	Migration struct {
		Name       string
		UpScript   string
		DownScript string
	}

	Migrate interface {
		GenerateDdl(schema string) []Migration
	}
)

const (
	ALTER_TABLE = "ALTER TABLE ONLY"

	ADD_CONSTRAINT = "ADD CONSTRAINT"

	FOREIGN_KEY = "FOREIGN KEY"

	CREATE_TABLE = "CREATE TABLE"

	CREATE_SEQUENCE = "CREATE SEQUENCE"

	CREATE_INDEX = "CREATE INDEX"

	SECURE_CREATE_TABLE = "CREATE TABLE IF NOT EXISTS"

	SECURE_CREATE_SEQUENCE = "CREATE SEQUENCE IF NOT EXISTS"

	SECURE_CREATE_INDEX = "CREATE INDEX IF NOT EXISTS"

	SECURE_DROP_VIEW = "DROP VIEW IF EXISTS %s;"

	SECURE_DROP_TYPE = "DROP TYPE IF EXISTS %s;"

	SECURE_DROP_SEQUENCE = "DROP SEQUENCE IF EXISTS %s;"

	SECURE_DROP_INDEX = "DROP INDEX IF EXISTS %s.%s;"

	SECURE_DROP_FUNCTION = "DROP FUNCTION IF EXISTS %s(%s);"

	SECURE_DROP_PRIMARY_KEY = "ALTER TABLE IF EXISTS ONLY %s.%s DROP CONSTRAINT IF EXISTS %s;"

	SECURE_DROP_FOREIGN_KEY = "ALTER TABLE IF EXISTS ONLY %s.%s DROP CONSTRAINT IF EXISTS %s;"

	SQL_CREATE_PRIMARY_KEY = "ALTER TABLE ONLY %s.%s ADD CONSTRAINT %s %s;"

	SQL_CREATE_FOREIGN_KEY = "ALTER TABLE ONLY %s.%s ADD CONSTRAINT %s %s;"

	SQL_CREATE_SEQUENCE = `
CREATE SEQUENCE IF NOT EXISTS %s
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;`

	SQL_CREATE_ENUM_OPEN = `
DO $$ BEGIN
    CREATE TYPE %s AS ENUM (`

	SQL_CREATE_ENUM_CLOSE = `%s);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;
    `

	SQL_CREATE_TABLE_OPEN  = "CREATE TABLE IF NOT EXISTS %s ("
	SQL_CREATE_TABLE_CLOSE = ");"

	QUERY_LIST_PRIMARY_KEY_IN_TABLE = `
SELECT
    conname AS primary_key,
    pg_get_constraintdef(oid) AS definition
FROM   pg_constraint
WHERE contype = 'p'
    AND connamespace = '%s'::regnamespace
    AND conrelid::regclass::text = '%s.%s'
ORDER BY conrelid::regclass::text,
    contype DESC;`

	QUERY_LIST_FOREIGN_KEY_IN_TABLE = `
SELECT
    conname AS foreign_key,
    pg_get_constraintdef(oid) AS definition
FROM   pg_constraint
WHERE contype = 'f'
    AND connamespace = '%s'::regnamespace
    AND conrelid::regclass::text = '%s.%s'
ORDER BY conrelid::regclass::text,
    contype DESC;`

	QUERY_LIST_COLUMN_IN_TABLE = `
SELECT
    column_name,
    data_type,
    character_maximum_length AS max_length,
    is_nullable,
    column_default
FROM
    information_schema.columns
WHERE table_schema = '%s' AND
    table_name = '%s';`

	QUERY_LIST_INDEX_IN_TABLE = `
SELECT
    indexname,
    indexdef
FROM
    pg_indexes
WHERE schemaname = '%s'
    AND tablename = '%s'
ORDER BY tablename,
    indexname;`

	QUERY_LIST_SEQUENCE = `
SELECT
	CONCAT(sequence_schema, '.', sequence_name) AS seq_name
FROM information_schema.sequences
WHERE sequence_schema = '%s';`

	QUERY_LIST_FUNCTION = `
SELECT
    p.proname AS function_name,
    pg_get_functiondef(p.oid) AS function_definition,
    pg_get_function_arguments(p.oid) AS function_paramters
FROM pg_proc p
JOIN pg_namespace n
    ON n.oid = p.pronamespace
WHERE n.nspname = '%s';`

	QUERY_LIST_ENUM = `
SELECT
    pg_catalog.format_type ( t.oid, NULL ) AS name,
    pg_catalog.array_to_string (
        ARRAY( SELECT e.enumlabel
                FROM pg_catalog.pg_enum e
                WHERE e.enumtypid = t.oid
                ORDER BY e.oid ), '#'
        ) AS values
FROM pg_catalog.pg_type t
LEFT JOIN pg_catalog.pg_namespace n
    ON n.oid = t.typnamespace
WHERE ( t.typrelid = 0
        OR ( SELECT c.relkind = 'c'
                FROM pg_catalog.pg_class c
                WHERE c.oid = t.typrelid
            )
    )
    AND NOT EXISTS
        ( SELECT 1
            FROM pg_catalog.pg_type el
            WHERE el.oid = t.typelem
                AND el.typarray = t.oid
        )
    AND n.nspname <> 'pg_catalog'
    AND n.nspname <> 'information_schema'
    AND n.nspname = '%s'
ORDER BY name;`

	QUERY_LIST_TABLE = `
SELECT
    LOWER(table_name) AS table_name
FROM information_schema.tables
WHERE table_type='BASE TABLE'
    AND table_schema='%s'
ORDER BY table_name;`

	QUERY_COUNT_TABLE = `
SELECT
    COUNT(1) as total
FROM information_schema.tables
WHERE table_type='BASE TABLE'
    AND table_schema='%s';`

	QUERY_LIST_VIEW = `
SELECT
    COALESCE(table_name, '') AS view_name,
    COALESCE(view_definition, '') AS definition
FROM information_schema.views
WHERE table_schema = '%s'
ORDER BY table_name;`

	QUERY_MATERIALIZED_VIEW = `
SELECT
    matviewname AS view_name,
    definition AS definition
FROM pg_matviews
WHERE schemaname = '%s'
ORDER BY schemaname,
    view_name;`
)
