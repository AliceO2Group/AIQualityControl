-- Usage example:
-- psql -U postgres -d your_database \
--   -v new_username='alice' \
--   -v new_password='supersecret' \
--   -v target_schema='public' \
--   -v readwrite='false' \
--   -f create_db_user.sql
--
-- For read/write:
-- psql -U postgres -d your_database \
--   -v new_username='bob' \
--   -v new_password='supersecret' \
--   -v target_schema='public' \
--   -v readwrite='true' \
--   -f create_db_user.sql

\set ON_ERROR_STOP on

DO $do$
DECLARE
    v_username text := :'new_username';
    v_password text := :'new_password';
    v_schema   text := COALESCE(NULLIF(:'target_schema', ''), 'public');
    v_readwrite boolean := lower(COALESCE(:'readwrite', 'false')) IN ('true','1','yes','y','on');
    v_db text := current_database();
BEGIN
    -- Create role if missing
    IF NOT EXISTS (
        SELECT 1
        FROM pg_roles
        WHERE rolname = v_username
    ) THEN
        EXECUTE format('CREATE ROLE %I WITH LOGIN PASSWORD %L', v_username, v_password);
    ELSE
        EXECUTE format('ALTER ROLE %I WITH LOGIN PASSWORD %L', v_username, v_password);
    END IF;

    -- Database-level access
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I', v_db, v_username);

    -- Schema-level access
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', v_schema, v_username);

    -- Existing objects
    EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO %I', v_schema, v_username);
    EXECUTE format('GRANT SELECT ON ALL SEQUENCES IN SCHEMA %I TO %I', v_schema, v_username);

    IF v_readwrite THEN
        EXECUTE format(
            'GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA %I TO %I',
            v_schema, v_username
        );
        EXECUTE format(
            'GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA %I TO %I',
            v_schema, v_username
        );
    END IF;

    -- Future objects created by the CURRENT role running this script
    EXECUTE format(
        'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO %I',
        v_schema, v_username
    );
    EXECUTE format(
        'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON SEQUENCES TO %I',
        v_schema, v_username
    );

    IF v_readwrite THEN
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO %I',
            v_schema, v_username
        );
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT USAGE, UPDATE ON SEQUENCES TO %I',
            v_schema, v_username
        );
    END IF;
END
$do$;

SELECT
    :'new_username' AS username,
    current_database() AS database_name,
    COALESCE(NULLIF(:'target_schema', ''), 'public') AS schema_name,
    CASE
        WHEN lower(COALESCE(:'readwrite', 'false')) IN ('true','1','yes','y','on')
        THEN 'read_write'
        ELSE 'read_only'
    END AS access_mode;
    