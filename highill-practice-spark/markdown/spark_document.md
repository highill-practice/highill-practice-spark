
# spark_document.md  

Spark SQL, Spark MLLib, Spark GraphX, Spark Streaming  

Spark master url: http://spark.apache.org/docs/2.1.0/submitting-applications.html#master-urls  

https://github.com/apache/spark/tree/master/examples/src/main/java/org/apache/spark/examples

https://github.com/apache/spark/tree/master/data/mllib  



# document  

- http://spark.apache.org/  
- http://spark.apache.org/docs/2.1.0/  
- http://spark.apache.org/docs/2.1.0/quick-start.html  
- http://spark.apache.org/docs/2.1.0/programming-guide.html  
- http://spark.apache.org/docs/2.1.0/streaming-programming-guide.html  
- http://spark.apache.org/docs/2.1.0/sql-programming-guide.html  
- http://spark.apache.org/docs/2.1.0/ml-guide.html  
- http://spark.apache.org/docs/2.1.0/graphx-programming-guide.html  



# Maven依赖  

- http://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11/2.1.1  
- http://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11/2.1.1  
- http://mvnrepository.com/artifact/org.apache.spark/spark-streaming_2.11/2.1.1  
- http://mvnrepository.com/artifact/org.apache.spark/spark-graphx_2.11/2.1.1  
- http://mvnrepository.com/artifact/org.apache.spark/spark-mllib_2.11/2.1.1  








# Spark SQL  

```  

mysql> create database demo_database;
Query OK, 1 row affected (0.00 sec)

mysql> show create database demo_database;
+---------------+------------------------------------------------------------------------+
| Database      | Create Database                                                        |
+---------------+------------------------------------------------------------------------+
| demo_database | CREATE DATABASE `demo_database` /*!40100 DEFAULT CHARACTER SET utf8 */ |
+---------------+------------------------------------------------------------------------+
1 row in set (0.00 sec)

mysql> use demo_database;


Database changed
mysql> create table demo_person(id bigint primary key not null auto_increment,
    -> name varchar(64),
    -> unique index unique_name(name));
Query OK, 0 rows affected (0.01 sec)



mysql> insert into demo_person(id, name) values (1001, 'Tom@qq.com');
Query OK, 1 row affected (0.00 sec)

mysql> insert into demo_person(id, name) values (1002, 'demo1002@qq.com');
Query OK, 1 row affected (0.00 sec)

mysql> insert into demo_person(id, name) values (1003, '测试1003');
Query OK, 1 row affected (0.00 sec)



```  



