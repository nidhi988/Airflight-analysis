#loading pre-processed data from pig to hive using HCatalog

hive --service metastore

#creating hive table
create table aviation(year int,month int,flight_num int,origin sring,destination string,cancelled int,cancel_code int,diversion int)
row format delimited fields terminated by '.'
stored as textfile;


#loading data to hive
store C into 'aviation' using org.apache.hive.hcatalog.pig.HCatStorer();

select * from aviation;

#processing data in hive

select month,COUNT(cancelled) as count from aviation
where cancelled=1 and cancel_code='B'
group by month 
order by count desc limit 5;


select origin,dest,count(diversion) as count
from aviation
where diversion=1
group by origin,dest
order by count desc limit 10;


select dest,count(dest) as x from aviation
group by dest
order by x desc limit 5;

