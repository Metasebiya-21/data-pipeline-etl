{% test unique_pair(model, left, right) %}
-- Each (left, right) pair must appear only once.
select {{ left }}, {{ right }}, count(*) as n
from {{ model }}
group by {{ left }}, {{ right }}
having count(*) > 1
{% endtest %}
