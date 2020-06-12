
--creamos la tabla
create external table if not exists input_table (member_id int,  name string,  email string,  joined int,  ip_address string,  posts int,  bday_day int,  bday_month int,  bday_year int,  members_profile_views int,  referred_by int) row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile location 'hdfs:///user/evinhas1/data' tblproperties ("skip.header.line.count"="1");


--Generate a query to obtain a most birthdays on 1 day
-- es extraño, porque como los datos no incluyen las fechas, la fecha con mayor numero de cumpleanhos es "null"

select max(no_bdays)
from (
select count(*) as no_bdays, concat(bday_year,bday_month, bday_day) as bday, bday_month from input_table
group by concat(bday_year,bday_month, bday_day), bday_month
) as t;


