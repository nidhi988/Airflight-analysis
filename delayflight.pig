#loading data into hdfs
hadoop fs -put /home/user/Delayedflights.csv/

#preprocessing in pig
A= LOAD '/Delayedflights.txt' using PigStorage(',');
B= FOREACH A GENERATE (int) S1 AS year,(int) S2 AS month, (int) S10 AS flight_num, (chararray) S17 AS origin,(chararray) S18 AS dest, (int) S22 AS cancelled,(chararray) S23 AS cancel_code,(int) S24 AS diversion;

describe B;

C=filter B BY(year is not null)AND(month is not null)AND(flight_num is not null)AND(origin is not null)AND(desr is not null)AND(cancelled is not null)AND(cancel_code is not null)AND(diversion is not null);


