# Cassandra

### Team member:
  Xingyu Yan,
  Jianhe Luo,
  Qing Li,
  Bowei Zhang.


### Usage Instruction:

#### 1. Connect local pc to BIC server:
- Open your terminal
- Type: ssh honeycomb@128.2.7.38
- Type in the password: ask teammates
- Type “exit” to exit this connection 

#### 2. Create table and insert data into Cassandra with CQL:
- Connect to BIC server
- CD into Cassandra-> apache-cassandra-2.1.9 -> bin
- Type: python cqlsh (To open cassandra)

#####Create keyspace:  
e.g. 
```
CREATE KEYSPACE demo
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
```
#####Create table:
Synopsis:
CREATE TABLE keyspace_name.table_name 
( column_definition, column_definition, ...)
WITH property AND property ...

e.g.
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

e.g.
```
INSERT INTO demo.users 
( user_name, gender )
VALUES ( 'John', 'Male' )
```
#### 3. Create table and insert data by using JDBC:

in-doing 

#### 4. Cassandra API

Synopsis: With Cassandra Java API, users do not need to write SQL to access the Cassandra. Users can write java code to connect Cassandra database, create table, insert objects, delete objects and get objects.



Instruction

* Create a CassandraDAOBuilder object
* Create a CassandraDAO object
* Using build() function in CassandraDAOBuilder object and pass CassandraDAO object as parameter to build up the setting
* Then you will get specific DAO java file and under folder "daos" and get specific model java file under folder "models".
* In folder "models", you can find out the data model you create.
* In folder "daos", you can find out the specific DAO java file, which contains some actions to operate Cassandra database.
* After that, you can call getInstance() function to a instance of specific data model, call createTable() function to create a new table for this data model, and call insert() function to insert a new object into the table.
