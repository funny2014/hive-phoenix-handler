#hive-phoenix-handler
## PhoenixStorageHandler for Hive 
============
hive-phoenix-handler is a hive plug-in that can access Apache Phoenix table on HBase using HiveQL.
My aim is solving Big-Big join issue of Apache Phoenix.
Apache Phoenix is a well-made solution whitch handling HBase using SQL. But, join-query between two big table is very very slow beyond endurance or impossible.
We already have proved query engine called Hive. Therefore there is no reason not to use it.
Moreover we solved full scan issue of hive by applying predicate push down and gained performance. 
Now, we are able to handle almost all the work(create/drop table, loading(insert)/update/delete data and query) using Hive CLI or Beeline without Phoenix CLI.

### Support Functions
============
* Phoenix table creation or deletion using HiveQL(DDL). 
* Data insert/update/delete using HiveQL(DML).
* Query for phoenix table or join query for phoenix tables or join query for phoenix table and hive table. 
* Applying predicate push down including salting table.
** Range scan available based row-key condition.
** Not Retrieving unmatching data by applying predicate push down on column condition also.
** Equal, not equal, equal or greater than, less than, equal or less than, between, not between, in, not in, is null, is not null operation support on predicate push down.
* Support mr and tez mode
* Support join between multi clusters

### Limitations
* Required modifying source of hive-ql package.
** org.apache.hadoop.hive.ql.io.HiveInputFormat Class : 
	In certain case join query is abnormal processed. because left hand side table's read column name is missing.
	Most will be fine if you don't modify.
** org.apache.hadoop.hive.ql.plan.TableScanDesc Class : 
	Changes in accordance with Hive-11609 patch.
	It must be modified.
** org.apache.hadoop.hive.ql.exec.Utilities Class : 
	Needed to control the number of reducers when hive.execution.engine is mr.
	If you do not modify, then reducer number is always one.
** org.apache.hadoop.hive.ql.optimizer.SetReducerParallelism Class : 
	Needed to control the number of reducers when hive.execution.engine is tez only single-table query.
	If you do not modify, then reducer number is always one.
** org.apache.hadoop.hive.ql.io.RecordIdentifier Class : 
	Needed to use update/delete statement on transactional table.
	If you don't modify, then you must give up update/delete statement. But insert statement still possible.
* Required modifying source of phoenix-core package.
** org.apache.phoenix.execute.MutationState Class : 
	Optional. But if you want performance burst on insert/update statement. Modify it.
** Let's ignore then time zone issue.

### Usage
============

#### [Apache Hive - 1.2.1 and Above/ Apache Phoenix 4.6 and above]

Add hive-phoenix-handler-<version>.jar to `hive.aux.jars.path`. If you don't modify artifact id of pom file. lgcns is prefixed.
And other jar file must be added because of version conflict of joda-time libaray. Phoenix & HBase(jruby-complete-1.6.8.jar) are using different version.
You also have to copy right yoda-time library(of phoenix. maybe ver 2.7) to $HBASE_HOME/lib.
To summarize, Adding jar file list to `hive.aux.jars.path` is
```
hive-phoenix-handler-<version>.jar
netty-all-4.0.23.Final.jar
hbase-server-1.0.1.1.jar
hbase-common-1.0.1.1.jar
hbase-client-1.0.1.1.jar
hbase-protocol-1.0.1.1.jar
phoenix-4.6.0-HBase-1.0-client-minimal.jar
joda-time-2.7.jar
```

### Compile
============
To compile the project 
mvn package
