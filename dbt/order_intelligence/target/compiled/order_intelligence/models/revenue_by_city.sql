select
  c.city,
  count(o.order_id) as orders,
  round(sum(case when o.status != 'canceled' then o.total_amount else 0 end), 2) as revenue,
  round(avg(case when o.status != 'canceled' then o.total_amount end), 2) as avg_order_value
from orders_current o
join customers_current c
  on o.customer_id = c.customer_id
group by c.city
order by revenue desc