{% test has_length_9(model, column_name) %}
-- Rows where the column is present but not exactly 9 characters long.
select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null and length({{ column_name }}) <> 9
{% endtest %}
