{% macro load_discount_data() %}

-- 🔍 Debug: Print dbt target values
{{ log("DBT Target Database: " ~ target.database, info=True) }}
{{ log("DBT Target Schema: " ~ target.schema, info=True) }}
{{ log("DBT Target Role: " ~ target.role, info=True) }}

-- 🔍 Debug: Check Snowflake session context
{% set context_query %}
    select current_database(), current_schema(), current_role()
{% endset %}

{% set context_result = run_query(context_query) %}

{% if execute %}
    {% set db_val = context_result.columns[0].values()[0] %}
    {% set schema_val = context_result.columns[1].values()[0] %}
    {% set role_val = context_result.columns[2].values()[0] %}

    {{ log("Snowflake Current DB: " ~ db_val, info=True) }}
    {{ log("Snowflake Current Schema: " ~ schema_val, info=True) }}
    {{ log("Snowflake Current Role: " ~ role_val, info=True) }}
{% endif %}


-- Use db & schema safely
{% set db = target.database %}
{% set schema = target.schema %}

-- Step 1: Create sequence
{% set seq_query %}
    create or replace sequence {{ db }}.{{ schema }}.WK_TEIRITSU_URIAGE_SEQ
    start = 1
    increment = 1
    order
{% endset %}

{% do run_query(seq_query) %}


-- Step 2: Count records
{% set count_query %}
    select count(*) as cnt
    from {{ source('raw', 'T_AGENT_DISCOUNT') }}
    where SIO_SEND_DT is null
{% endset %}

{% set results = run_query(count_query) %}

{% if execute %}
    {% set row_count = results.columns[0].values()[0] %}
{% else %}
    {% set row_count = 0 %}
{% endif %}

{% if row_count == 0 %}

    {{ log("No data to process", info=True) }}

{% else %}

    {{ log("Processing " ~ row_count ~ " rows", info=True) }}

    -- INSERT
    {% set insert_query %}
        insert into {{ db }}.{{ schema }}.WK_TEIRITSU_URIAGE
        select
            T_NO,
            CYUMON_NUSI_CD,
            case when SURYO > 0 then '1' else '0' end,
            N_SYORI_YMD,
            SOUKO_C,
            BUKA_C,
            TANTOUSYA_C,
            URIAGE_NO,
            NULL,
            NULL,
            CYUMON_NUSI_CD,
            NULL,
            '000',
            JUCYU_NO,
            'C0310',
            KOU_NO,
            '7F1',
            HINMEI_C,
            '   ',
            SURYO,
            0,
            0,
            NEBIKI_TANKA,
            NULL,
            {{ db }}.{{ schema }}.WK_TEIRITSU_URIAGE_SEQ.NEXTVAL,
            0,
            0,
            0,
            CURRENT_TIMESTAMP
        from {{ source('raw', 'T_AGENT_DISCOUNT') }}
        where SIO_SEND_DT is null
    {% endset %}

    {% do run_query(insert_query) %}

    -- UPDATE
    {% set update_query %}
        update {{ source('raw', 'T_AGENT_DISCOUNT') }}
        set SIO_SEND_DT = current_timestamp
        where SIO_SEND_DT is null
    {% endset %}

    {% do run_query(update_query) %}

{% endif %}

{% endmacro %}