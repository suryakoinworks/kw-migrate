package db

type (
	migration struct {
		Name       string
		UpScript   string
		DownScript string
	}

	Migrate interface {
		GenerateDdl(schema string) []migration
	}
)

const (
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

	QUERY_LIST_VIEW = `
SELECT
    COALESCE(table_name, '') AS view_name,
    COALESCE(view_definition, '') AS definition
FROM information_schema.views
WHERE table_schema = '%s'
ORDER BY table_name;`
)
