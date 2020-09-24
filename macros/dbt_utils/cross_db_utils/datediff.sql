{% macro spark__datediff(first_date, second_date, datepart) %}

    {% if datepart == 'day' %}

        datediff(date({{second_date}}), date({{first_date}}))
        
    {% elif datepart == 'week' %}

        floor(datediff(date({{second_date}}), date({{first_date}}))/7)

    {% elif datepart == 'month' %}

        floor(months_between(date({{second_date}}), date({{first_date}})))
        
    {% elif datepart == 'quarter' %}
    
        floor(months_between(date({{second_date}}), date({{first_date}}))/3)
        
    {% elif datepart == 'year' %}
    
        floor(months_between(date({{second_date}}), date({{first_date}}))/12)

    {% elif datepart in ('hour', 'minute', 'second', 'millisecond', 'microsecond') %}
    
        {%- set divisor -%} 
            {%- if datepart == 'hour' -%} 3600
            {%- elif datepart == 'minute' -%} 60
            {%- elif datepart == 'second' -%} 1
            {%- elif datepart == 'millisecond' -%} (1/1000)
            {%- elif datepart == 'microsecond' -%} (1/1000000)
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
