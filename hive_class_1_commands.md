# HQL Command to Show Databases
show databases;

# HQL Command to Create Database
create database hive_db;

# HQL Command to Use Database
use hive_db;

# HQL Command to Create Table For CSV Data
create table department_data
(
dept_id int,
dept_name string,
manager_id int,
salary int)
row format delimited
fields terminated by ',';

# HQL Command to get details of Hive Table
describe formatted department_data;

# HQL Command to Load Data in Hive table from local file system
load data local inpath 'file:///home/workspace/depart_data.csv' into table department_data;

# HQL Property to display column names
set hive.cli.print.header = true;

# HQL command to drop Hive table
drop table department_data;

# HQL Command to load data in Hive table from HDFS path
create table department_data
(
dept_id int,
dept_name string,
manager_id int,
salary int)
row format delimited
fields terminated by ',';

load data inpath '/tmp/input_data/depart_data.csv' into table department_data;

# HQL Command to create external table in Hive
create external table department_date_external 
(
dept_id int, 
dept_name string, 
manager_id int, 
salary int) 
row format delimited 
fields terminated by ',' 
location '/tmp/input_data/';

# HQL Command to handle Array data types
create table employee
(
id int,
name string,
skills array<string>
)
row format delimited
fields terminated by ','
collection items terminated by ':';

load data local inpath 'file:///config/workspace/array_data.csv' into table employee;

# HQL Command to get element by index in hive array data type
select id, name, skills[0] as prime_skill from employee;

# Functions for array data type
select
id,
name,
size(skills) as size_of_each_array,
array_contains(skills,"HADOOP") as knows_hadoop,
sort_array(skills) as sorted_array
from employee;


# HQL Command to handle Map data types
create table employee_map_data
(
id int,
name string,
details map<string,string>
)
row format delimited
fields terminated by ','
collection items terminated by '|'
map keys terminated by ':';

load data local inpath 'file:///config/workspace/map_data.csv' into table employee_map_data;

select
id,
name,
details["gender"] as employee_gender
from employee_map_data;

# Functions for map data type
select
id,
name,
details,
size(details) as size_of_each_map,
map_keys(details) as distinct_map_keys,
map_values(details) as distinct_map_values
from employee_map_data;
