set tez.queue.name=arabdprd_q1;
set hive.execution.engine=tez;

set hive.auto.convert.join.noconditionaltask.size = 10000000;
set hive.support.concurrency = true;
set hive.enforce.bucketing = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.compactor.initiator.on = true;
set hive.compactor.worker.threads = 1 ;

drop table if exists ara.employer_all;

CREATE TABLE
    ara.employer_all AS
SELECT
    e.*,
    a.address_type_id,
    a.address_line_1,
    a.address_line_2,
    a.city,
    a.state_cd,
    a.country_cd,
    a.zip_cd
FROM
    employer e,
    employer_address a
WHERE
    a.employer_id = e.employer_id;
CLUSTERED BY
    (
        state_cd
    )
INTO
    50 BUCKETS STORED AS ORC TBLPROPERTIES
    (
        "transactional"="true",
        "skip.header.line.count"="1", 
        "compactor.mapreduce.map.memory.mb"="2048",
        "compactorthreshold.hive.compactor.delta.num.threshold"="4",
        "compactorthreshold.hive.compactor.delta.pct.threshold"="0.5" 
    );