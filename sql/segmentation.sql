--queue and engine
set tez.queue.name=arabdprd_q1;
set hive.execution.engine=tez;

--acid tables
set hive.auto.convert.join.noconditionaltask.size = 10000000;
set hive.support.concurrency = true;
set hive.enforce.bucketing = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.compactor.initiator.on = true;
set hive.compactor.worker.threads = 1 ;

--dynamic partitioning
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict

DROP TABLE IF EXISTS ara.employer_all;

set CREATE_TABLE_QUERY=SELECT e.*, a.address_type_id, a.address_line_1, a.address_line_2, a.city, a.state_cd, a.country_cd, a.zip_cd FROM ara.employer e, ara.employer_address a WHERE a.employer_id = e.employer_id;
CREATE TABLE ara.employer_all STORED AS ORC AS ${hiveconf:CREATE_TABLE_QUERY} limit 0;

ALTER TABLE ara.employer_all  CLUSTERED BY (state_cd) INTO 50 BUCKETS;
ALTER TABLE ara.employer_all  SET TBLPROPERTIES (
"transactional"="true",
"compactor.mapreduce.map.memory.mb"="2048",
"compactorthreshold.hive.compactor.delta.num.threshold"="4",
"compactorthreshold.hive.compactor.delta.pct.threshold"="0.5" 
);

${hiveconf:CREATE_TABLE_QUERY};