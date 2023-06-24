show databases;

create database hive_db;

use hive_db;

create table department_data
(
dept_id int,
dept_name string,
manager_id int,
salary int)
row format delimited
fields terminated by ',';

describe formatted department_data;

For data load from local
load data local inpath 'file:///config/workspace/depart_data.csv' into table department_data;
