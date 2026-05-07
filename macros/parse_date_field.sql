{% macro parse_date_field(col) %}
    case
        when btrim({{ col }}) <> ''
            and (
                btrim({{ col }}) ~ '^\d{4}-\d{2}-\d{2}$'
                or btrim({{ col }}) ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
            )
        then to_date(
            case
                when btrim({{ col }}) ~ '^\d{4}-\d{2}-\d{2}$'
                    then btrim({{ col }})
                else regexp_replace(btrim({{ col }}), '\s*-\s*', '-', 'g')
            end,
            case
                when btrim({{ col }}) ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                else 'DD-Mon-YYYY'
            end
        )
    end
{% endmacro %}


{% macro milestone_delay_percent(planned, actual) %}
    case
        when {{ planned }} is null or {{ actual }} is null then null
        when ({{ actual }} - {{ planned }}) = 0  then 100.0
        when ({{ actual }} - {{ planned }}) >= 1  then 100.0 - ((({{ actual }} - {{ planned }})::numeric / 30) * 100)
        when ({{ actual }} - {{ planned }}) <= -1 then 100.0 + ((({{ actual }} - {{ planned }})::numeric / 30) * 100)
    end
{% endmacro %}
