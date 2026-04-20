
  
    
    

    create  table
      "order_intelligence"."main"."orders_summary__dbt_tmp"
  
    as (
      select
  status,
  count(*) as order_count,
  round(sum(total_amount), 2) as revenue,
  round(avg(total_amount), 2) as avg_order_value
from orders_current
where status is not null
group by status
order by revenue desc
    );
  
  