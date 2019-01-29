# Hive's quick start with Yosegi

## Preparation
We have a plan to create a docker environment of Hadoop and Hive for test use, but current situation, you need to prepare Hadoop and Hive firstly.

- [Apache Hadoop](https://hadoop.apache.org)
- [Apache Hive](https://hive.apache.org/)

# Get Yosegi's jars
Get Yosegi's jar from Maven repository.It can be easily obtained from the following command.

  $ ./bin/get_jar.sh get

It is in the following path.
```
./jars/yosegi/latest/yosegi.jar
./jars/yosegi/latest/yosegi-hive.jar
```

Copy this jar to an arbitrary path and add it to Hive's "add jar".
Please refer to the following for the command of Hive.

```
add jar /tmp/yosegi.jar;
add jar /tmp/yosegi-hive.jar;
``

## Preparation of input data and create Hive table
In this example, CSV data is assumed to be input data.
With this data as input, we create a table in Yosegi format and read data from the table.

Create the following csv file.In this example, to illustrate writing to and reading from the table, the data is a simple example.
If you already have a table, you still have the input.

```
id,name,age
X_0001,AAA,20
X_0002,BBB,30
X_0003,CCC,32
X_0004,DDD,21
X_0005,EEE,28
X_0006,FFF,21
```

Run Hive.
Create a table for csv created in the example.

```
create table example (
  id string, 
  name string, 
  age int )
row format delimited fields terminated by ',' lines terminated by '\n'
tblproperties ('skip.header.line.count'='1');
```

Load the created file into this table.

```
load data local inpath 'example.csv' overwrite into table example;
```

## Create Yosegi table
Create a table of Yosegi. Serde, InputFormat, OutputFormat are as follows.

```
create table example_yosegi (
  id string, 
  name string, 
  age int )
  ROW FORMAT SERDE
    'jp.co.yahoo.yosegi.hive.YosegiSerde'
  STORED AS INPUTFORMAT
    'jp.co.yahoo.yosegi.hive.io.YosegiHiveLineInputFormat'
  OUTPUTFORMAT
    'jp.co.yahoo.yosegi.hive.io.YosegiHiveParserOutputFormat';
```

Load the data from the CSV table created earlier.

```
insert into table example_yosegi select * from example;

hive> select * from example_yosegi;
OK
X_0001  AAA     20
X_0002  BBB     30
X_0003  CCC     32
X_0004  DDD     21
X_0005  EEE     28
X_0006  FFF     21
```

# What if I want to know more?

