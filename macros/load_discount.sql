{% macro load_discount_data() %}

{{ log("STEP 1: Using role", info=True) }}
{% do run_query("USE ROLE " ~ target.role) %}

{{ log("STEP 2: Using warehouse", info=True) }}
{% do run_query("USE WAREHOUSE " ~ target.warehouse) %}

{{ log("STEP 3: Using database", info=True) }}
{% do run_query("USE DATABASE " ~ target.database) %}

{{ log("STEP 4: Using schema", info=True) }}
{% do run_query("USE SCHEMA " ~ target.schema) %}

{{ log("STEP 5: Check table T_AGENT_DISCOUNT", info=True) }}
{% do run_query("SELECT COUNT(*) FROM " ~ target.database ~ "." ~ target.schema ~ ".T_AGENT_DISCOUNT") %}

{{ log("STEP 6: Check table WK_TEIRITSU_URIAGE", info=True) }}
{% do run_query("SELECT COUNT(*) FROM " ~ target.database ~ "." ~ target.schema ~ ".WK_TEIRITSU_URIAGE") %}

{{ log("STEP 7: Create sequence", info=True) }}
{% do run_query("CREATE OR REPLACE SEQUENCE " ~ target.database ~ "." ~ target.schema ~ ".WK_TEIRITSU_URIAGE_SEQ START = 1 INCREMENT = 1") %}

{{ log("STEP 8: Done till sequence", info=True) }}

{% endmacro %}