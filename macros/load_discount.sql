{% macro load_discount_data() %}

-- 🔥 Step 0: Force full session context
{% do run_query("USE ROLE " ~ target.role) %}
{% do run_query("USE WAREHOUSE " ~ target.warehouse) %}
{% do run_query("USE DATABASE " ~ target.database) %}
{% do run_query("USE SCHEMA " ~ target.schema) %}

-- 🔍 Debug context
{{ log("=== DBT CONTEXT ===", info=True) }}
{{ log("Database: " ~ target.database, info=True) }}
{{ log("Schema: " ~ target.schema, info=True) }}
{{ log("Role: " ~ target.role, info=True) }}
{{ log("Warehouse: " ~ target.warehouse, info=True) }}

{% set ctx_query %}
    select current_database(), current_schema(), current_role(), current_warehouse()
{% endset %}

{% set ctx = run_query(ctx_query) %}

{% if execute %}
    {{ log("Snowflake DB: " ~ ctx.columns[0].values()[0], info=True) }}
    {{ log("Snowflake Schema: " ~ ctx.columns[1].values()[0], info=True) }}
    {{ log("Snowflake Role: " ~ ctx.columns[2].values()[0], info=True) }}
    {{ log("Snowflake Warehouse: " ~ ctx.columns[3].values()[0], info=True) }}
{% endif %}

{% set db = target.database %}
{% set schema = target.schema %}

-- 🔍 Step 1: Validate tables exist
{% set check_tables %}
    select table_name 
    from {{ db }}.information_schema.tables
    where table_schema = upper('{{ schema }}')
      and table_name in ('T_AGENT_DISCOUNT','WK_TEIRITSU_URIAGE')
{% endset %}

{% set tbls = run_query(check_tables) %}

{% if execute %}
    {{ log("Tables found: " ~ tbls.columns[0].values(), info=True) }}
{% endif %}

-- 🔍 Step 2: Create sequence
{% set seq_query %}
    create or replace sequence {{ db }}.{{ schema }}.WK_TEIRITSU_URIAGE_SEQ
    start = 1
    increment = 1
    order
{% endset %}

{{ log("Creating sequence...", info=True) }}
{% do run_query(seq_query) %}

-- 🔍 Step 3: Count records
{% set count_query %}
    select count(*) as cnt
    from {{ db }}.{{ schema }}.T_AGENT_DISCOUNT
    where SIO_SEND_DT is null
{% endset %}

{% set results = run_query(count_query) %}

{% if execute %}
    {% set row_count = results.columns[0].values()[0] %}
    {{ log("Row count: " ~ row_count, info=True) }}
{% else %}
    {% set row_count = 0 %}
{% endif %}

-- 🔍 Step 4: Conditional execution
{% if row_count == 0 %}

    {{ log("No data to process", info=True) }}

{% else %}

    {{ log("Processing rows...", info=True) }}

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
        from {{ db }}.{{ schema }}.T_AGENT_DISCOUNT
        where SIO_SEND_DT is null
    {% endset %}

    {{ log("Running INSERT...", info=True) }}
    {% do run_query(insert_query) %}

    -- UPDATE
    {% set update_query %}
        update {{ db }}.{{ schema }}.T_AGENT_DISCOUNT
        set SIO_SEND_DT = current_timestamp
        where SIO_SEND_DT is null
    {% endset %}

    {{ log("Running UPDATE...", info=True) }}
    {% do run_query(update_query) %}

{% endif %}

{{ log("=== MACRO COMPLETED SUCCESSFULLY ===", info=True) }}

{% endmacro %}