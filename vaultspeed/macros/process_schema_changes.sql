{% macro process_schema_changes(on_schema_change, source_relation, target_relation) %}

    {% if on_schema_change == 'ignore' %}
    {# VaultSpeed-- If we ignore, use the columns from the source table  to create the insert instead of the columns from the target. This prevents erors when there are more columns in the target than the source. #}

     {{ return(adapter.get_columns_in_relation(source_relation)) }}

    {% else %}

      {% set schema_changes_dict = check_for_schema_changes(source_relation, target_relation) %}

      {% if schema_changes_dict['schema_changed'] %}

        {% if on_schema_change == 'fail' %}

          {% set fail_msg %}
              The source and target schemas on this incremental model are out of sync!
              They can be reconciled in several ways:
                - set the `on_schema_change` config to either append_new_columns or sync_all_columns, depending on your situation.
                - Re-run the incremental model with `full_refresh: True` to update the target schema.
                - update the schema manually and re-run the process.
          {% endset %}

          {% do exceptions.raise_compiler_error(fail_msg) %}

        {# -- unless we ignore, run the sync operation per the config #}
        {% else %}

          {% do sync_column_schemas(on_schema_change, target_relation, schema_changes_dict) %}

        {% endif %}

      {% endif %}

      {{ return(schema_changes_dict['source_columns']) }}

    {% endif %}

{% endmacro %}