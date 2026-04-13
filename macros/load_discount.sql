{% macro load_discount_data() %}

-- Step 1: Create sequence
{% set seq_query %}
    create or replace sequence WK_TEIRITSU_URIAGE_SEQ
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

-- Step 3: Condition
{% if row_count == 0 %}

    {{ log("No data to process", info=True) }}

{% else %}

    {{ log("Processing " ~ row_count ~ " rows", info=True) }}

    -- Step 4: INSERT (NOW EXECUTED)
    {% set insert_query %}
        insert into WK_TEIRITSU_URIAGE
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
            WK_TEIRITSU_URIAGE_SEQ.NEXTVAL,
            0,
            0,
            0,
            CURRENT_TIMESTAMP
        from {{ source('raw', 'T_AGENT_DISCOUNT') }}
        where SIO_SEND_DT is null
    {% endset %}

    {% do run_query(insert_query) %}

    -- Step 5: UPDATE (NOW EXECUTED)
    {% set update_query %}
        update {{ source('raw', 'T_AGENT_DISCOUNT') }}
        set SIO_SEND_DT = current_timestamp
        where SIO_SEND_DT is null
    {% endset %}

    {% do run_query(update_query) %}

{% endif %}

{% endmacro %}