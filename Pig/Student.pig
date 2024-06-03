std_detail = LOAD '/home/exam/pig-0.17.0/pigprog/data.txt'
USING PigStorage(',') as (id:int, firstname:chararray, lastname:chararray, age:int, phone:chararray,city:chararray);
filter_data = FILTER std_detail BY city == 'Chennai';
group_data = GROUP std_detail by age;
STORE filter_data INTO 'filter';
STORE group_data INTO 'group';
