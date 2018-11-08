use project3_test;

show databases;

show tables in project3_test;

set mapreduce.job.queuename=araadh_q1.aratech_sq1;
set hive.auto.convert.join.noconditionaltask.size = 10000000;
set hive.support.concurrency = true;
set hive.enforce.bucketing = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.compactor.initiator.on = true;
set hive.compactor.worker.threads = 1 ;

select * from locations limit 100;
select * from secure_locations limit 100;

-- locations stage
drop table locations_stg;
create table locations_stg(city varchar(256),
city_ascii varchar(256),
state_id varchar(2),
state_name varchar(256),
county_fips int,
county_name varchar(256),
lat double,
lng double,
population bigint,
population_proper bigint,
density int,
source varchar(100),
incorporated boolean,
timezone varchar(100),
zips varchar(100),
id int,
group_id int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");

-- locations
drop table locations;
create table locations(city varchar(256),
city_ascii varchar(256),
state_id varchar(2),
state_name varchar(256),
county_fips int,
county_name varchar(256),
lat double,
lng double,
population bigint,
population_proper bigint,
density int,
source varchar(100),
incorporated boolean,
timezone varchar(100),
zips varchar(100),
id int,
group_id int)
CLUSTERED BY (state_name) INTO 50 BUCKETS STORED AS ORC
TBLPROPERTIES ("transactional"="true",
  "skip.header.line.count"="1",
  "compactor.mapreduce.map.memory.mb"="2048",     -- specify compaction map job properties
  "compactorthreshold.hive.compactor.delta.num.threshold"="4",  -- trigger minor compaction if there are more than 4 delta directories
  "compactorthreshold.hive.compactor.delta.pct.threshold"="0.5" -- trigger major compaction if the ratio of size of delta files to
                                                                   -- size of base files is greater than 50%
);

load data inpath '/user/eperler/uscitiesv1.4.csv' into table locations_stg;

create view secure_locations AS select d.* from locations d inner join permissions p on d.group_id=p.gid where username = current_user();

select d.* from locations d inner join permissions p on d.group_id=p.gid where username = current_user() limit 10;
