with urls as (
    select 'http://example.com/?utm_source=google&gclid=abc123&x=1' as url, 'gclid' as param, 'abc123' as expected
    union all select 'http://example.com/?gclid=first&utm_source=google', 'gclid', 'first'
    union all select 'http://example.com/?a=1&utm_source=last', 'utm_source', 'last'
    union all select 'http://example.com/?a=1', 'gclid', cast(null as string)
    union all select 'http://example.com/?gclid=&a=1', 'gclid', cast(null as string)
)

select
    url,
    param,
    expected,
    case param
        when 'gclid' then {{ dbt_utils.get_url_parameter('url', 'gclid') }}
        when 'utm_source' then {{ dbt_utils.get_url_parameter('url', 'utm_source') }}
    end as actual
from urls
