
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select city
from "order_intelligence"."main"."revenue_by_city"
where city is null



  
  
      
    ) dbt_internal_test