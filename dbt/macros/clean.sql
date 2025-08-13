-- dbt/macros/clean.sql

{% macro clean_text(col) -%}
(
    NULLIF(
        TRIM(BOTH FROM REGEXP_REPLACE({{ col }}::text, '^\s*NULL\s*$', '', 'i')),
        ''
    )
)
{%- endmacro %}

{% macro clean_int(col) -%}
(
    CASE
        WHEN {{ col }} IS NULL THEN NULL
        -- treat NULL-like strings as NULL
        WHEN UPPER(TRIM({{ col }}::text)) IN ('', 'NULL', 'N/A', 'NA') THEN NULL
        ELSE NULLIF(
            REGEXP_REPLACE({{ col }}::text, '[^0-9]', '', 'g'),
            ''
        )::INTEGER
    END
)
{%- endmacro %}

{#
  clean_numeric:
  - Removes spaces and quote-like thousands markers (’, ′, ″, ´).
  - Handles US "1,234.56", EU "1.234,56", integer with separators,
    and simple "1234,56" or "1234.56".
#}
{% macro clean_numeric(col) -%}
(
    CASE
        WHEN {{ col }} IS NULL THEN NULL
        WHEN UPPER(TRIM({{ col }}::text)) IN ('', 'NULL', 'N/A', 'NA') THEN NULL
        ELSE (
            WITH __raw AS (
                SELECT TRIM({{ col }}::text) AS s0
            ),
            __norm0 AS (
                -- Normalize unicode minus (U+2212) to ASCII hyphen
                SELECT REPLACE(s0, '−', '-') AS s0a
                FROM __raw
            ),
            __norm1 AS (
                -- Remove spaces and quote-like chars: ’ U+2019, ′ U+2032, ″ U+2033, ´ U+00B4
                SELECT TRANSLATE(s0a, ' ’′″´', '') AS s1
                FROM __norm0
            ),
            __eu AS (
                -- EU style with decimals: 1.234,56 or 1234,56 (with optional sign)
                SELECT CASE
                    WHEN s1 ~ '^[-+]?\d{1,3}(\.\d{3})+,\d+$'
                      OR (s1 ~ '^[-+]?\d+,\d+$' AND s1 NOT LIKE '%,%,%')
                    THEN REPLACE(REPLACE(s1, '.', ''), ',', '.')
                    ELSE NULL
                END AS val
                FROM __norm1
            ),
            __us AS (
                -- US style with decimals: 1,234.56 or integers with commas: 1,234 (optional sign)
                SELECT CASE
                    WHEN s1 ~ '^[-+]?\d{1,3}(,\d{3})+\.\d+$'
                    THEN REPLACE(s1, ',', '')
                    WHEN s1 ~ '^[-+]?\d{1,3}(,\d{3})+$'
                    THEN REPLACE(s1, ',', '')
                    ELSE NULL
                END AS val
                FROM __norm1
            ),
            __simple AS (
                -- Dot decimal, comma decimal, or plain digits (optional sign)
                SELECT CASE
                    WHEN s1 ~ '^[-+]?\d+\.\d+$' THEN s1
                    WHEN s1 ~ '^[-+]?\d+,\d+$'  THEN REPLACE(s1, ',', '.')
                    WHEN s1 ~ '^[-+]?\d+$'      THEN s1
                    ELSE NULL
                END AS val
                FROM __norm1
            )
            SELECT NULLIF(COALESCE(__eu.val, __us.val, __simple.val), '')::NUMERIC
            FROM __eu, __us, __simple
            LIMIT 1
        )
    END
)
{%- endmacro %}

{#
  clean_date:
  - Handles: YYYY-MM-DD or YYYY/MM/DD
             DD.MM.YYYY
             MM/DD/YYYY vs DD/MM/YYYY (auto-choose by first chunk)
             NULL-likes
#}
{% macro clean_date(col) -%}
(
    CASE
        WHEN {{ col }} IS NULL THEN NULL
        WHEN UPPER(TRIM({{ col }}::text)) IN ('', 'NULL', 'N/A', 'NA') THEN NULL

        -- 2024-06-23 or 2024/06/23
        WHEN TRIM({{ col }}::text) ~ '^\d{4}[-/]\d{2}[-/]\d{2}$'
            THEN TO_DATE(REPLACE(TRIM({{ col }}::text), '/', '-'), 'YYYY-MM-DD')

        -- 23.06.2024
        WHEN TRIM({{ col }}::text) ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN TO_DATE(TRIM({{ col }}::text), 'DD.MM.YYYY')

        -- 06/13/2024 vs 13/06/2024 -> infer from first part
        WHEN TRIM({{ col }}::text) ~ '^\d{2}/\d{2}/\d{4}$'
            THEN CASE
                    WHEN SPLIT_PART(TRIM({{ col }}::text), '/', 1)::INT > 12
                        THEN TO_DATE(TRIM({{ col }}::text), 'DD/MM/YYYY')
                    ELSE TO_DATE(TRIM({{ col }}::text), 'MM/DD/YYYY')
                 END
        ELSE NULL
    END
)
{%- endmacro %}
