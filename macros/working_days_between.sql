{#
    working_days_between(start_date, end_date)

    Whole working days (Mon-Fri) elapsed between two date expressions.

    Counts the half-open interval [start_date, end_date): a same-day turnaround is 0 working
    days, Thursday -> Friday is 1, and Friday -> Monday is 1. Weekends are skipped. PUBLIC
    HOLIDAYS ARE NOT SKIPPED, because this project holds no holiday calendar; if one is seeded
    later, subtract the holidays that fall on a weekday inside the interval here and every
    caller picks up the correction.

    Closed form rather than generate_series so the macro stays a plain scalar expression that
    can sit in any select list. For any date d, ((d - epoch) / 7) * 5 + least((d - epoch) % 7, 5)
    is the count of weekdays strictly before d, measured from the epoch 1900-01-01, which was a
    Monday. Differencing the two counts leaves the weekdays inside the interval. Both dates are
    assumed to fall on or after that epoch.

    Returns NULL when either endpoint is NULL, and a NEGATIVE number when end_date precedes
    start_date, so the caller decides whether an out-of-order pair is acceptable.
#}

{% macro working_days_between(start_date, end_date) %}
case
    when ({{ start_date }}) is not null and ({{ end_date }}) is not null
        then (
            (((({{ end_date }})::date - date '1900-01-01') / 7) * 5
                + least(((({{ end_date }})::date - date '1900-01-01') % 7), 5))
            - (((({{ start_date }})::date - date '1900-01-01') / 7) * 5
                + least(((({{ start_date }})::date - date '1900-01-01') % 7), 5))
        )
end
{% endmacro %}
