{#
    financial_year_start_year(date_expr)

    Starting calendar year of the Indian financial year (April-March) that a date falls in.
    April 2024 -> 2024, March 2025 -> 2024, April 2025 -> 2025.

    Returns an INTEGER rather than a label so it can be used as a sort key and as the single
    input to financial_year_label below. Every caller that needs both should derive the year
    once in an earlier CTE and format it later, which is what the Finance models already do.

    Returns NULL when the date is NULL, and also when it falls outside
    [2015-01-01, current_date + 365]. That guard is deliberate: the Zoho exports carry a
    handful of corrupt dates (employee_bill_line_item_report holds 2 rows dated year 0002)
    which would otherwise land silently in a bogus financial year instead of reading as
    unknown. The upper bound allows a year of legitimate forward-dating.
#}

{% macro financial_year_start_year(date_expr) %}
case
    when ({{ date_expr }})::date between date '2015-01-01' and current_date + 365
        then case
            when extract(month from ({{ date_expr }})::date) >= 4
                then extract(year from ({{ date_expr }})::date)::integer
            else extract(year from ({{ date_expr }})::date)::integer - 1
        end
end
{% endmacro %}


{#
    financial_year_label(start_year_expr, format)

    Renders a financial year start year (see financial_year_start_year) as display text.

    The April-March rule is reimplemented inline across this project in several mutually
    incompatible output formats, so the format is an explicit argument rather than a default:
    a caller that picks the wrong one silently changes how a dashboard groups. Formats, for a
    start year of 2024:

      'zoho_long' -> '01st April 2024 to 31st March 2025'
                     matches the text Zoho itself stores in Budget_Line_Item_Master_Report's
                     Financial_Year column, so a derived FY can be compared to the master's
                     own without parsing either side.
      'fy_short'  -> 'FY 2024-25'
      'long'      -> '2024-2025'
      'short'     -> '24-25'

    Returns NULL when the start year is NULL, so an unknown FY never renders as a real one.
    Raises at compile time on an unrecognised format rather than emitting NULL, since a typo
    here would otherwise blank out a whole column unnoticed.
#}

{% macro financial_year_label(start_year_expr, format) %}
{%- if format == 'zoho_long' -%}
    {%- set rendered %}
        '01st April ' || ({{ start_year_expr }})::text
        || ' to 31st March ' || (({{ start_year_expr }}) + 1)::text
    {% endset -%}
{%- elif format == 'fy_short' -%}
    {%- set rendered %}
        'FY ' || ({{ start_year_expr }})::text
        || '-' || lpad(((({{ start_year_expr }}) + 1) % 100)::text, 2, '0')
    {% endset -%}
{%- elif format == 'long' -%}
    {%- set rendered %}
        ({{ start_year_expr }})::text || '-' || (({{ start_year_expr }}) + 1)::text
    {% endset -%}
{%- elif format == 'short' -%}
    {%- set rendered %}
        lpad((({{ start_year_expr }}) % 100)::text, 2, '0')
        || '-' || lpad(((({{ start_year_expr }}) + 1) % 100)::text, 2, '0')
    {% endset -%}
{%- else -%}
    {{ exceptions.raise_compiler_error(
        "financial_year_label: unknown format '" ~ format
        ~ "'. Expected one of 'zoho_long', 'fy_short', 'long', 'short'."
    ) }}
{%- endif -%}
case
    when ({{ start_year_expr }}) is not null
        then {{ rendered | trim }}
end
{% endmacro %}
