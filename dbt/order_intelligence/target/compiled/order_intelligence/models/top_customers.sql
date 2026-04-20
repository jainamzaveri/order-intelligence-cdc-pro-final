select
  c.customer_id,
  c.customer_name,
  c.city,
  c.loyalty_tier,
  count(o.order_id) as num_orders,
  round(sum(case when o.status != 'canceled' then o.total_amount else 0 end), 2) as total_spent,
  round(avg(case when o.status != 'canceled' then o.total_amount end), 2) as avg_order_value
from orders_current o
join customers_current c
  on o.customer_id = c.customer_id
group by c.customer_id, c.customer_name, c.city, c.loyalty_tier
order by total_spent desc
limit 20