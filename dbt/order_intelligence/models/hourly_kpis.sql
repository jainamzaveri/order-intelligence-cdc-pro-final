select
  date_trunc('hour', order_ts) as order_hour,
  count(*) as orders,
  round(sum(case when status != 'canceled' then total_amount else 0 end), 2) as revenue,
  round(avg(case when status != 'canceled' then total_amount end), 2) as avg_order_value
from orders_current
group by 1
order by 1
