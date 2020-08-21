{% macro spark__datediff(first_date, second_date, datepart) %}

    {% if datepart == 'day' %}

        datediff(date({{second_date}}), date({{first_date}}))

    {% elif datepart == 'month' %}

        floor(months_between(date({{second_date}}), date({{first_date}})))
        
    {% elif datepart == 'year' %}
    
        floor(months_between(date({{second_date}}), date({{first_date}}))/12)

    {% elif datepart in ('hour', 'minute', 'second') %}
    
        {%- set divisor -%} 
            {%- if datepart == 'hour' -%} 3600
            {%- elif datepart == 'minute' -%} 60
            {%- else -%} 1
            {%- endif -%}
        {%- endset -%}

        floor(
            (to_unix_timestamp({{second_date}}) 
            - to_unix_timestamp({{first_date}})
        ) / {{divisor}})

    {% else %}

        {{ exceptions.raise_compiler_error("macro datediff not implemented for datepart ~ '" ~ datepart ~ "' ~ on Spark") }}

    {% endif %}

{% endmacro %}
