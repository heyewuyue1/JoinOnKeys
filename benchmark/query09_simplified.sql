select  (select count(*)
                  from store_sales 
                  where ss_quantity between 1 and 20) bucket1,
        (select count(*)
                  from store_sales
                  where ss_quantity between 21 and 40) bucket2
from reason
where r_reason_sk = 1
;


