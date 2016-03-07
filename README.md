#hive-phoenix-handler
## PhoenixStorageHandler for Hive 
hive-phoenix-handler is a hive plug-in that can access Apache Phoenix table on HBase using HiveQL.
My aim is solving Big-Big join issue of Apache Phoenix.
Apache Phoenix is a well-made solution whitch handling HBase using SQL. But, join-query between two big table is very very slow beyond endurance or impossible.
We already have proved query engine called Hive. Therefore there is no reason not to use it.
Moreover we solved full scan issue of hive by applying predicate push down and gained performance. 
Now, we are able to handle almost all the work(create/drop table, loading(insert)/update/delete data and query) using Hive CLI or Beeline without Phoenix CLI.

### Support Functions
* Phoenix table creation or deletion using HiveQL(DDL). 
* Data insert/update/delete using HiveQL(DML).
* Query for phoenix table or join query for phoenix tables or join query for phoenix table and hive table. 
* Applying predicate push down including salting table.
  * Range scan available based row-key condition.
  * Not Retrieving unmatching data by applying predicate push down on column condition also.
  * Equal, not equal, equal or greater than, less than, equal or less than, between, not between, in, not in, is null, is not null operation support on predicate push down.
* Can read phoenix index table because it use phoenix compiler & optimizer.
* Support mr and tez mode
* Support join between multi clusters

### Limitations
* Required modifying source of hive-ql package.
  * org.apache.hadoop.hive.ql.io.HiveInputFormat Class : 
  >	In certain case join query is abnormal processed. because left hand side table's read column name is missing.
  >	Most will be fine if you don't modify.
  * org.apache.hadoop.hive.ql.plan.TableScanDesc Class : 
  >	Changes in accordance with Hive-11609 patch.
  >	It must be modified.
  * org.apache.hadoop.hive.ql.exec.Utilities Class : 
  >	Needed to control the number of reducers when hive.execution.engine is mr.
  >	If you do not modify, then reducer number is always one.
  * org.apache.hadoop.hive.ql.optimizer.SetReducerParallelism Class : 
  >	Needed to control the number of reducers when hive.execution.engine is tez only single-table query.
  >	If you do not modify, then reducer number is always one.
  * org.apache.hadoop.hive.ql.io.RecordIdentifier Class : 
  >	Needed to use update/delete statement on transactional table.
  >	If you don't modify, then you must give up update/delete statement. But insert statement still possible.
* Required modifying source of phoenix-core package.
  * org.apache.phoenix.execute.MutationState Class : 
  >	Optional. But if you want performance boost on insert/update statement. Modify it.
  * Let's ignore then time zone issue.

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
#### Create Table
It can create table on not exist table in HBase or create external table on exist table in HBase.

##### Create Table when Phoenix table deos not exist in HBase
```SQL
create table phoenix_table (
  r1 string,
  r2 int,
  r3 date,
  r4 timestamp,
  c1 boolean,
  c2 tinyint,
  c3 smallint,
  c4 int,
  c5 bigint,
  c6 float,
  c7 double,
  c8 decimal(7,2),
  c9 char(10),
  c10 date,
  c11 timestamp,
  c13 binary
)
STORED BY 'org.apache.phoenix.hive.PhoenixStorageHandler'
TBLPROPERTIES (
  "phoenix.table.name" = "phoenix_table",
  "phoenix.zookeeper.quorum" = "<zookeeper quorums>",
  "phoenix.zookeeper.znode.parent" = "/hbase",
  "phoenix.zookeeper.client.port" = "2181",
  "phoenix.rowkeys" = "r1, r2, r3, r4",
  "phoenix.column.mapping" = "c1:c1,c2:c2,c3:c3,c4:c4,c5:c5,c6:c6,c7:c7,c8:c8,c9:c9,c10:A.c10,c11:A.c11,c12:A.c12,c13:A.c13(100)",
  "phoenix.table.options" = "SALT_BUCKETS=10, DATA_BLOCK_ENCODING='DIFF',A.VERSIONS=3"
);
```
`phoenix.table.name` property can be omitted if same name of hive.
`phoenix.zookeeper.quorum`, `phoenix.zookeeper.znode.parent`, `phoenix.zookeeper.client.port` properties also omitable using default value. Each default value is `localhost`, `/hbase`, `2181`.
`phoenix.rowkeys` property is mandatory.
`phoenix.column.mapping` property can be optional if you want create columns using default column-family and same column name of hive. Otherwise you specify column-family name and phoenix column name. Text format is <hive-column>:<column-family>.<phoenix-column>. In the case of binary type column must be specified the length of data.
Phoenix table options is written to `phoenix.table.options` property.
If you decide to use all default value. then 
```
create table phoenix_table (
  r1 string,
  r2 int,
  r3 date,
  r4 timestamp,
  c1 boolean,
  c2 tinyint,
  c3 smallint,
  c4 int,
  c5 bigint,
  c6 float,
  c7 double,
  c8 decimal(7,2),
  c9 char(10),
  c10 date,
  c11 timestamp
)
STORED BY 'org.apache.phoenix.hive.PhoenixStorageHandler'
TBLPROPERTIES (
  "phoenix.rowkeys" = "r1, r2, r3, r4"
);
```
so simple.

##### Create External Table when Phoenix table exist in HBase
```
create external date_dim (
  d_date_sk                 INT,
  d_date_id                 VARCHAR(16),
  d_date                    DATE,
  d_month_seq               INT,
  d_week_seq                INT,
  d_quarter_seq             INT,
  d_year                    INT,
  d_dow                     INT,
  d_moy                     INT,
  d_dom                     INT,
  d_qoy                     INT,
  d_fy_year                 INT,
  d_fy_quarter_seq          INT,
  d_fy_week_seq             INT,
  d_day_name                VARCHAR(9),
  d_quarter_name            VARCHAR(6),
  d_holiday                 VARCHAR(1),
  d_weekend                 VARCHAR(1),
  d_following_holiday       VARCHAR(1),
  d_first_dom               INT,
  d_last_dom                INT,
  d_same_day_ly             INT,
  d_same_day_lq             INT,
  d_current_day             VARCHAR(1),
  d_current_week            VARCHAR(1),
  d_current_month           VARCHAR(1),
  d_current_quarter         VARCHAR(1),
  d_current_year            VARCHAR(1)
)
STORED BY 'org.apache.phoenix.hive.PhoenixStorageHandler'
TBLPROPERTIES (
  "phoenix.table.name" = "date_dim",
  "phoenix.zookeeper.quorum" = "<zookeeper quorums>",
  "phoenix.zookeeper.znode.parent" = "/hbase",
  "phoenix.zookeeper.client.port" = "2181"
)
;
```
If table name of hive & phoenix, then `phoenix.table.name` property will be omitted.
The rest properties are same described above.

##### Create Transactional Table for use update/delete statement.
You create transactional table for use update/delete sql. In case of insert statement doen't matter. And transaction table can be created external table or not external table.
The object of transactional table is only for use update/delete statement.
```
create external table inventory
(
  inv_date_sk int,
  inv_item_sk int,
  inv_warehouse_sk int,
  inv_quantity_on_hand int
)
STORED BY 'org.apache.phoenix.hive.PhoenixStorageHandler'
TBLPROPERTIES (
  "transactional" = "true"
);
```
If you use update/delete statement on non-transactional table. NPE will be occurred.

#### Load Data
To load data to phoenix table through hive, use insert statement. Be careful. Exception occurred if you use function in insert ~ values statement.
```
FAILED: SemanticException [Error 10293]: Unable to create temp file for insert values Expression of type TOK_FUNCTION not supported in insert/values
```
Therefore only constant value is used in insert statement.
```
insert into table inv values (1, 2, 3, 4);
```
If you want to use function then you have to use insert ~ select statement.
```
insert into table inventory select 1, cast('2' as int), cast(rand() * 100 as int), 4 from dual;
```
Dual table is any table that you must create.
```
create table dual (value string);
insert into table dual values ('1');
```
Storing one data is no fun. Phoenix also has bulk loading capabilities but hive-phoenix-handler supports more interesting things that filtering or aggregation data.
If you have CSV/TSV files. then you can create external table in hive. Now you can insert all or specific data to phoenix table using select statement of hive. 
```
insert into table inventory
select * from ext_inv;
```
```
insert into table inventory
select * from ext_inv where inv_quantity_on_hand > 1000;
```
```
insert into table inventory
select inv_date_sk, min(inv_item_sk), max(inv_warehouse_sk), avg(inv_quantity_on_hand) from inventory group by inv_date_sk;
```
parameters on performance.

Parameters | Default Value | Description
------------ | ------------- | -------------
phoenix.upsert.batch.size | 1000 | Batch size for upsert.
[phoenix-table-name].disable.wal | false | It temporarily modify table attribute to `DISABLE_WAL = true`. And skip validation for performance boost.
[phoenix-table-name].auto.flush | false | When WAL is disabled and if this value is true. Then flush memstore to hfile.

#### Query Data
You can use HiveQL for querying data on phoenix table. A single table query as fast as Phoenix CLI when `hive.fetch.task.conversion=more` and `hive.exec.parallel=true`.

Parameters | Default Value | Description
------------ | ------------- | -------------
hbase.scan.cache | 100 | Read row size for an unit request.
hbase.scan.cacheblock | false | Whether or not cache block.
split.by.stats | false | If true, Many mapper is loaded using stat table of phoenix. One guide post is one mapper.
[hive-table-name].reducer.count | 1 | Number of reducer. In tez mode is affected only single-table query.
[phoenix-table-name].query.hint | | Hint for phoenix query. NO_INDEX, ... Reference phoenix documentation.

Query 82 on TPCDS 100G
```
select
  i.i_item_id
  ,i.i_item_desc
  ,i.i_current_price
from 
  p_date_dim d, p_item i, p_store_sales s, p_inventory inv
where 
  i.i_current_price between 44 and 44 + 30
  and i.i_manufact_id in (62,48,16,37)
  and inv.key.inv_item_sk = i.i_item_sk
  and d.d_date_sk = inv.key.inv_date_sk
  and d.d_date > TO_DATE('1998-06-13')
  and d.d_date < date_add(TO_DATE('1998-06-13'), 60)
  and inv.inv_quantity_on_hand between 100 and 500
  and s.key.ss_item_sk = i.i_item_sk
group by 
  i.i_item_id,i.i_item_desc,i.i_current_price
order by 
  i.i_item_id
```
This query does not run in Phoenix. But hive-phoenix-handler can do with reasonable performance.

### Compile
To compile the project 
mvn package
