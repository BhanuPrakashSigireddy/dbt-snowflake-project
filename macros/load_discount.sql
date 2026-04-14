{% macro load_discount_data() %}

-- 🔍 Debug
{{ log("DB: " ~ target.database, info=True) }}
{{ log("Schema: " ~ target.schema, info=True) }}
{{ log("Role: " ~ target.role, info=True) }}

{% set db = target.database %}
{% set schema = target.schema %}

-- STEP 1: Check access to source table
{{ log("Checking source table...", info=True) }}
{% do run_query("SELECT COUNT(*) FROM " ~ db ~ "." ~ schema ~ ".T_AGENT_DISCOUNT") %}

-- STEP 2: Check access to target table
{{ log("Checking target table...", info=True) }}
{% do run_query("SELECT COUNT(*) FROM " ~ db ~ "." ~ schema ~ ".WK_TEIRITSU_URIAGE") %}

-- STEP 3: Create sequence (fully qualified)
{{ log("Creating sequence...", info=True) }}
{% do run_query(
    "CREATE OR REPLACE SEQUENCE " ~ db ~ "." ~ schema ~ ".WK_TEIRITSU_URIAGE_SEQ START = 1 INCREMENT = 1"
) %}

-- STEP 4: Count records
{% set count_query %}
    SELECT COUNT(*) FROM {{ db }}.{{ schema }}.T_AGENT_DISCOUNT
    WHERE SIO_SEND_DT IS NULL
{% endset %}

{% set result = run_query(count_query) %}

{% if execute %}
    {% set row_count = result.columns[0].values()[0] %}
    {{ log("Rows to process: " ~ row_count, info=True) }}
{% else %}
    {% set row_count = 0 %}
{% endif %}

-- STEP 5: Process
{% if row_count == 0 %}

    {{ log("No data to process", info=True) }}

{% else %}

    {{ log("Running INSERT...", info=True) }}

    {% do run_query(
        "INSERT INTO " ~ db ~ "." ~ schema ~ ".WK_TEIRITSU_URIAGE " ~
        "SELECT T_NO, CYUMON_NUSI_CD, " ~
        "CASE WHEN SURYO > 0 THEN '1' ELSE '0' END, " ~
        "N_SYORI_YMD, SOUKO_C, BUKA_C, TANTOUSYA_C, URIAGE_NO, " ~
        "NULL, NULL, CYUMON_NUSI_CD, NULL, '000', JUCYU_NO, 'C0310', " ~
        "KOU_NO, '7F1', HINMEI_C, '   ', SURYO, 0, 0, NEBIKI_TANKA, NULL, " ~
        db ~ "." ~ schema ~ ".WK_TEIRITSU_URIAGE_SEQ.NEXTVAL, " ~
        "0, 0, 0, CURRENT_TIMESTAMP " ~
        "FROM " ~ db ~ "." ~ schema ~ ".T_AGENT_DISCOUNT " ~
        "WHERE SIO_SEND_DT IS NULL"
    ) %}

    {{ log("Running UPDATE...", info=True) }}

    {% do run_query(
        "UPDATE " ~ db ~ "." ~ schema ~ ".T_AGENT_DISCOUNT " ~
        "SET SIO_SEND_DT = CURRENT_TIMESTAMP " ~
        "WHERE SIO_SEND_DT IS NULL"
    ) %}

{% endif %}

{{ log("✅ MACRO COMPLETED", info=True) }}

{% endmacro %}