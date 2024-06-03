customers = LOAD '/home/exam/pig-0.17.0/cust/data.txt' USING PigStorage(',') as (id:int, name:chararray, age:int,address:chararray, salary:int);
orders = LOAD 'order.txt' USING PigStorage(',') as (oid:int, date:chararray, customer_id:int,amount:int);
join_result = JOIN customers BY id, orders BY customer_id;
STORE join_result INTO 'joinoutput';
customers = LOAD '/home/exam/pig-0.17.0/cust/data.txt' USING PigStorage(',') as (id:int, name:chararray, age:int,address:chararray, salary:int);
orders = LOAD 'order.txt' USING PigStorage(',') as (oid:int, date:chararray, customer_id:int,amount:int);
join_result = JOIN customers BY id, orders BY customer_id;
sorting = ORDER join_result BY age ASC;
STORE sorting INTO 'sortoutput';
