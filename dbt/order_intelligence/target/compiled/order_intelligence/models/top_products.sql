select
  p.product_id,
  p.product_name,
  p.category,
  sum(oi.quantity) as units_sold,
  round(sum(case when o.status != 'canceled' then oi.line_amount else 0 end), 2) as revenue
from order_items_current oi
join orders_current o
  on oi.order_id = o.order_id
join products_current p
  on oi.product_id = p.product_id
group by p.product_id, p.product_name, p.category
order by revenue desc
limit 20