BEGIN;

CREATE SCHEMA IF NOT EXISTS dt;
SET SEARCH_PATH = dt;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS hstore;

-- setup ----------------------------------------------

CREATE FUNCTION tuid_zero() RETURNS UUID
  IMMUTABLE
  LANGUAGE sql
AS
$$
SELECT '00000000-0000-0000-0000-000000000000' :: UUID
$$;

CREATE OR REPLACE FUNCTION tuid_generate() RETURNS UUID
  LANGUAGE plpgsql
AS
$$
DECLARE
  ct BIGINT;
  r BYTEA;
  r0 BIGINT;
  r1 BIGINT;
  r2 BIGINT;
  ax BIGINT;
  bx BIGINT;
  cx BIGINT;
  dx BIGINT;
  ret VARCHAR;
BEGIN
  r := "gen_random_bytes"(8); -- we use 58 bits of this

  r0 := (get_byte(r, 0) << 8) | get_byte(r, 1);
  r1 := (get_byte(r, 2) << 8) | get_byte(r, 3);

  -- The & mask here is to suppress the sign extension on the 32nd bit.
  r2 := ((get_byte(r, 4) << 24) | (get_byte(r, 5) << 16) | (get_byte(r, 6) << 8) | get_byte(r, 7)) &
    x'0FFFFFFFF'::BIGINT;

  ct := extract(EPOCH FROM clock_timestamp() AT TIME ZONE 'utc') * 1000000;

  ax := ct >> 32;
  bx := ct >> 16 & x'FFFF' :: INT;
  cx := x'4000' :: INT | ((ct >> 4) & x'0FFF' :: INT);
  dx := x'8000' :: INT | ((ct & x'F' :: INT) << 10) | ((r0 >> 6) & x'3F' :: INT);

  ret :=
    LPAD(TO_HEX(ax), 8, '0') ||
      LPAD(TO_HEX(bx), 4, '0') ||
      LPAD(TO_HEX(cx), 4, '0') ||
      LPAD(TO_HEX(dx), 4, '0') ||
      LPAD(TO_HEX(r1), 4, '0') ||
      LPAD(TO_HEX(r2), 8, '0');

  RETURN ret :: UUID;
END;
$$;

CREATE FUNCTION create_event_apply(schema_n VARCHAR, table_n VARCHAR)
  RETURNS VOID
  LANGUAGE plpgsql
AS
$$
DECLARE
  columns VARCHAR[];
  primary_key_columns VARCHAR[];
  distinct_columns VARCHAR[]; -- columns - pk_columns - ui_columns
  primary_key_columns_literal VARCHAR;
  event_apply_sql VARCHAR;
  do_sql VARCHAR;
  distinct_columns_set_literal VARCHAR;
  distinct_table_columns_literal VARCHAR;
  distinct_excluded_columns_literal VARCHAR;
  distinct_columns_is_distinct_from_literal VARCHAR;
BEGIN
  SELECT
    array_agg(column_name :: VARCHAR)
  INTO columns
  FROM
    information_schema.columns c
  WHERE
    c.table_schema = schema_n
    AND c.table_name = table_n;

  SELECT
    array_agg(a.attname)
  INTO primary_key_columns
  FROM
    pg_index x
      JOIN pg_class c ON c.oid = x.indrelid
      JOIN pg_class i ON i.oid = x.indexrelid
      LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY (x.indkey)
      LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE
    ((c.relkind = ANY (ARRAY ['r'::"char", 'm'::"char"])) AND (i.relkind = 'i'::"char"))
    AND x.indisprimary
    AND n.nspname = schema_n
    AND c.relname = table_n;

  SELECT
    array_agg(v)
  INTO distinct_columns
  FROM
    unnest(columns) a(v)
  WHERE
    v != ALL (primary_key_columns);

  SELECT
    array_to_string(array_agg(FORMAT('%I', v)), ', ')
  INTO primary_key_columns_literal
  FROM
    unnest(primary_key_columns) a(v);

  do_sql = 'DO NOTHING';

  IF cardinality(distinct_columns) > 0 THEN
    SELECT
      array_to_string(array_agg(FORMAT('%I = excluded.%I', v, v)), ', ')
    INTO distinct_columns_set_literal
    FROM
      unnest(distinct_columns) a(v);

    SELECT
      array_to_string(array_agg(FORMAT('%I.%I', table_n, v)), ', ')
    INTO distinct_table_columns_literal
    FROM
      unnest(distinct_columns) a(v);

    SELECT
      array_to_string(array_agg(FORMAT('excluded.%I', v)), ', ')
    INTO distinct_excluded_columns_literal
    FROM
      unnest(distinct_columns) a(v);

    distinct_columns_is_distinct_from_literal = FORMAT(
      '       (%s)
          IS DISTINCT FROM
              (%s)',
      distinct_table_columns_literal,
      distinct_excluded_columns_literal
      );

    do_sql = FORMAT(
      '   DO UPDATE SET
              %s
          WHERE
              %s',
      distinct_columns_set_literal,
      distinct_columns_is_distinct_from_literal
      );
  END IF;

  -- no rows affected here could mean the data wasn't changed or the update
  -- failed... it's a bit of a hole in the model. :shrug:
  event_apply_sql = FORMAT(
    $Q$
      CREATE OR REPLACE FUNCTION event_apply_%s(domain_id_ UUID, tx_log_id_ UUID)
        RETURNS VOID
        LANGUAGE plpgsql
      AS
      $func$
      DECLARE
        table_row_type %s%%ROWTYPE;
        domain_object HSTORE;
        updated_at_ TIMESTAMPTZ;
      BEGIN
        SELECT HSTORE(%s) INTO domain_object FROM %s WHERE %s = domain_id_;
        updated_at_ = domain_object->'updated_at';

        domain_object = event_apply(
          '%s',
          domain_id_,
          tx_log_id_,
          coalesce(domain_object, HSTORE('%s', domain_id_ :: TEXT))
          );
        IF domain_object IS NULL THEN
          DELETE FROM %s WHERE %s = domain_id_ AND updated_at = updated_at_;
        ELSE
          table_row_type = populate_record(table_row_type, domain_object);
          table_row_type.updated_at = now();

          INSERT INTO %s
          VALUES (table_row_type.*)
          ON CONFLICT (%s)
            %s;
        END IF;
      END;
      $func$
    $Q$,
    table_n,
    table_n,
    table_n, table_n, primary_key_columns_literal,
    table_n,
    primary_key_columns_literal,
    table_n, primary_key_columns_literal,
    table_n,
    primary_key_columns_literal,
    do_sql
    );
  RAISE NOTICE '%', event_apply_sql;
  EXECUTE event_apply_sql;
END;
$$;

-- tx's can avoid specifying original values by using an updated_at check instead. Pass the updated_at of the entity as seen and if it
-- matches we'll assumes the values last seen are all the same and do the changes. This avoids having to send and validate all the data
-- when doing deletes.
CREATE DOMAIN event_op AS CHAR(1)
  CHECK (value IN (
                   '+', -- set an attribute on an entity, requires the current value be NULL or updated_at matches (entity auto-vivified or updated)
                   '-', -- clear an attribute on an entity, requires the current value of the attribute to change or updated_at matches
                   'x' -- delete an entity, requires updated_at matches
    ));


CREATE TABLE tx_log
(
  tx_log_id UUID DEFAULT tuid_generate() PRIMARY KEY,
  who VARCHAR NOT NULL,
  what VARCHAR NOT NULL,
  notes VARCHAR NOT NULL DEFAULT '',
  tz TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE OR REPLACE FUNCTION tx_declare(who_ VARCHAR, what_ VARCHAR, notes_ VARCHAR)
  RETURNS UUID
  LANGUAGE sql
AS
$sql$
INSERT INTO
  tx_log (who, what, notes)
VALUES
  (who_, what_, notes_)
RETURNING tx_log_id;
$sql$;

CREATE TABLE event_log
(
  domain VARCHAR NOT NULL,
  domain_id UUID NOT NULL,
  kv HSTORE NOT NULL,
  op EVENT_OP NOT NULL,
  tx_log_id UUID NOT NULL REFERENCES tx_log,
  seq INTEGER NOT NULL
);

CREATE INDEX event_log_domain_id_tx_op_sq
  ON event_log (domain, domain_id, tx_log_id, op, seq);

CREATE FUNCTION make_event_atom(domain_ VARCHAR,
                                domain_id_ UUID,
                                kv_ HSTORE,
                                op_ EVENT_OP) RETURNS event_log -- partially filled,
  LANGUAGE plpgsql
AS
$$
DECLARE
  x event_log%rowtype;
BEGIN
  IF domain_ IS NULL OR domain_id_ IS NULL OR kv_ IS NULL OR op_ IS NULL
  THEN
    RAISE EXCEPTION 'no nulls allowed';
  END IF;
  x.domain = domain_;
  x.domain_id = domain_id_;
  x.kv = kv_;
  x.op = op_;
  RETURN x;
END;
$$;

CREATE FUNCTION event_hstore_verify(current HSTORE, expected HSTORE)
  RETURNS BOOLEAN
  LANGUAGE sql
AS
$sql$
  -- expect no unmatched pairs
SELECT
  NOT exists(
    SELECT
      1
    FROM
      each((
        SELECT
          slice( -- limit to keys in expected
            hstore(current),
            akeys(hstore(expected))
            ) -- remove matching pairs
            - hstore(expected)
      ))
    )
$sql$;

CREATE FUNCTION event_hstore_verify_are_null(current HSTORE, expected_to_be_null HSTORE)
  RETURNS BOOLEAN
  LANGUAGE sql
AS
$sql$
  -- expect no unmatched pairs
SELECT
  NOT exists(
    SELECT
      1
    FROM
      each((
        SELECT
          slice( -- limit to keys in expected_to_be_null
            hstore(current),
            akeys(hstore(expected_to_be_null))
            )
      )) x
    WHERE
      x.value IS NOT NULL
    )
$sql$;

CREATE FUNCTION event_apply(domain_ VARCHAR, domain_id_ UUID, tx_log_id_ UUID, current HSTORE)
  RETURNS HSTORE
  LANGUAGE plpgsql
AS
$$
DECLARE
  r RECORD;
  ua_matches BOOLEAN;
BEGIN
  FOR r IN
    SELECT *
    FROM
      event_log
    WHERE
      domain = domain_
      AND domain_id = domain_id_
      AND tx_log_id = tx_log_id_
    ORDER BY seq
    LOOP
      ua_matches = r.kv -> 'updated_at' = current -> 'updated_at'; -- can be NULL if r.kv doesn't provide updated_at

      IF r.op = '+' THEN
        IF NOT (ua_matches IS TRUE OR event_hstore_verify_are_null(current, r.kv)) THEN
          RAISE EXCEPTION 'event_op[+] mismatched: current=% expected=%', current, r.kv;
        END IF;
        current = current || r.kv;
        RAISE NOTICE '+ %', current;
      ELSIF r.op = '-' THEN
        IF  NOT (ua_matches IS TRUE OR event_hstore_verify(current, r.kv)) THEN
          RAISE EXCEPTION 'event_op[-] mismatched: current=% expected=%', current, r.kv;
        END IF;
        current = current - akeys(r.kv);
      ELSIF r.op = 'x' THEN
        IF ua_matches IS NOT TRUE THEN
          RAISE EXCEPTION 'event_op[x] mismatched updated_at: current=% expected=%', current, r.kv;
        END IF;
        RETURN NULL; -- stop, since there is no point in processing further entries as we're about to delete the entity.
      ELSE
        RAISE EXCEPTION 'invalid op: %', r.op;
      END IF;
    END LOOP;

  RETURN current;
END;
$$;

CREATE FUNCTION event_declare(arr event_log[], tx_log_id_ UUID)
  RETURNS UUID
  LANGUAGE plpgsql AS
$asdf$
DECLARE
  r RECORD;
BEGIN
  -- convert partially filled event_log entries into real ones
  INSERT
  INTO
    event_log (domain, domain_id, kv, op, tx_log_id, seq)
  SELECT
    a.domain,
    a.domain_id,
    a.kv,
    a.op,
    tx_log_id_,
    row_number() OVER ()
  FROM
    unnest(arr) a(domain, domain_id, kv, op);

  FOR r IN
    SELECT a.domain, a.domain_id FROM unnest(arr) a(domain, domain_id) GROUP BY a.domain, a.domain_id
    LOOP
      -- dynamically execute update based on entity domain
      EXECUTE format('select event_apply_%s(%L,%L)', r.domain, r.domain_id, tx_log_id_);
    END LOOP;

  RETURN tx_log_id_;
END;
$asdf$;

-- usage ----------------------------------------

CREATE TABLE users
(
  user_id UUID NOT NULL PRIMARY KEY,
  first_name VARCHAR NOT NULL,
  last_name VARCHAR NOT NULL,
  dob DATE,
  email VARCHAR NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_event_apply('dt', 'users');

SELECT
  event_declare(
    ARRAY [
      make_event_atom(
        'users',
        '0005a516-7584-4590-9036-adb9328b5f8a',
        hstore(ARRAY [
          'first_name', 'bob',
          'last_name', 'smith',
          'email', 'bob_smith@example.com',
          'dob', '1969-03-23'
          ]),
        '+')
      ],
    tx_declare('admin@example.com', 'USER_NEW', 'self sign-up')
    );

SELECT *
FROM
  users;

SELECT
  event_declare(
    ARRAY [
      make_event_atom('users', '0005a516-7584-4590-9036-adb9328b5f8a', hstore('first_name', 'bob'), '-'),
      make_event_atom('users', '0005a516-7584-4590-9036-adb9328b5f8a', hstore('first_name', 'robert'), '+')
      ],
    tx_declare('admin@example.com', 'USER_RENAME', 'change to legal name')
    );

SELECT *
FROM
  users;

SELECT
  event_declare(
    ARRAY [
      make_event_atom('users', '0005a516-7584-4590-9036-adb9328b5f8a', hstore(ARRAY [
        'updated_at', (SELECT updated_at::TEXT FROM users WHERE user_id = '0005a516-7584-4590-9036-adb9328b5f8a'),
        'last_name', 'johnson'
        ]), '+'),
      make_event_atom('users', '0005a516-7584-4590-9036-adb9328b5f8a', hstore(ARRAY [
        'updated_at', (SELECT updated_at::TEXT FROM users WHERE user_id = '0005a516-7584-4590-9036-adb9328b5f8a'),
        'dob', '' -- any allowed, since updated_at check overrides.
        ]), '-')
      ],
    tx_declare('admin@example.com', 'USER_RENAME', 'change to legal name')
    );

SELECT *
FROM
  users;

SELECT
  event_declare(
    ARRAY [
      make_event_atom('users', '0005a516-7584-4590-9036-adb9328b5f8a', hstore('updated_at', (SELECT updated_at::TEXT FROM users WHERE user_id = '0005a516-7584-4590-9036-adb9328b5f8a')), 'x') ],
    tx_declare('admin@example.com', 'USER_DELETE', 'goodbye bob')
    );

SELECT *
FROM
  users;

SELECT *
FROM
  event_log
    JOIN tx_log USING (tx_log_id)
ORDER BY
  tx_log_id, seq;

ROLLBACK;
