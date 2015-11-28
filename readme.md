# Cassandra

### Team member:
  Xingyu Yan,
  Jianhe Luo,
  Qing Li,
  Bowei Zhang.


### Usage Instruction:

#### 1. Connect local pc to BIC server:
- Open your terminal
- Type: ssh bicadmin@128.2.7.38
- Type in the password: J0hn!rOck3
- Type “exit” to exit this connection 

#### 2. Create table and insert data into Cassandra with CQL:
- Connect to BIC server
- CD into Cassandra-> apache-cassandra-2.1.9 -> bin
- Type: python cqlsh (To open cassandra)

#####Create keyspace:  
eg. 
```
CREATE KEYSPACE demo
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
```
#####Create table:
Synopsis:
CREATE TABLE keyspace_name.table_name 
( column_definition, column_definition, ...)
WITH property AND property ...

eg.
```
CREATE TABLE demo.users (
  user_name varchar PRIMARY KEY,
  gender varchar
);
```
#####Insert data:
Synopsis:
INSERT INTO keyspace_name.table_name
( column_name, column_name...)
VALUES ( value, value ... )
USING option AND option

eg.
```
INSERT INTO demo.users 
( user_name, gender )
VALUES ( 'John', 'Male' )
```
#### 3. Create table and insert data by using JDBC:

in-doing 
