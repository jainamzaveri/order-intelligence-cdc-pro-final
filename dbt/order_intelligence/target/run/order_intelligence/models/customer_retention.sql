
  
    
    

    create  table
      "order_intelligence"."main"."customer_retention__dbt_tmp"
  
    as (
      with customer_orders as (
  select
    customer_id,
    count(*) as total_orders,
    round(sum(case when status != 'canceled' then total_amount else 0 end), 2) as lifetime_revenue
  from orders_current
  group by customer_id
)
select
  c.customer_id,
  c.customer_name,
  c.city,
  c.loyalty_tier,
  co.total_orders,
  co.lifetime_revenue,
  case when co.total_orders > 1 then 'repeat' else 'one_time' end as customer_type
from customer_orders co
join customers_current c
  on co.customer_id = c.customer_id
order by co.lifetime_revenue desc
    );
  
  