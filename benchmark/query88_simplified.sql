select  *
from
    (select count(*) h8_30_to_9
     from store_sales, household_demographics , time_dim, store
     where ss_sold_time_sk = time_dim.t_time_sk
       and ss_hdemo_sk = household_demographics.hd_demo_sk
       and ss_store_sk = s_store_sk
       and time_dim.t_hour = 8
       and time_dim.t_minute >= 30
       and ((household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2) or
            (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
            (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2))
       and store.s_store_name = 'ese') s1,
    (select count(*) h9_to_9_30
     from store_sales, household_demographics , time_dim, store
     where ss_sold_time_sk = time_dim.t_time_sk
       and ss_hdemo_sk = household_demographics.hd_demo_sk
       and ss_store_sk = s_store_sk
       and time_dim.t_hour = 9
       and time_dim.t_minute < 30
       and ((household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2) or
            (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
            (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2))
       and store.s_store_name = 'ese') s2
;
