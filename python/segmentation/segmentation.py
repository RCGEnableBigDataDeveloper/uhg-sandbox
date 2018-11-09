import sys
import json
import subprocess

from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from templates.templates import Template


""" Generate segmentation tables

This module demonstrates how to automate segmentation of data and applying
authorization using Hive SQL Standard Authoriaztion.

Example:
   The following json structure...

    {
        "type": "auth",
        "resources": [{
            "name": "BASE_TABLE",
            "segments": [{
                "name": "TABLE_000",
                "segment": "000",
                "groups": ["g1", "g2", "g3"]
            },{
                "name": "TABLE_001",
                "segment": "001",
                "groups": ["g4", "g5", "g6"]
            },{
                "name": "TABLE_002",
                "segment": "002",
                "groups": ["g7", "g8", "g9"]
            }]
        }]
    }

will execute the following SQL statements against hive...

DROP TABLE IF EXISTS TABLE_000
CREATE TABLE TABLE_000  AS SELECT * FROM BASE_TABLE WHERE ID=000
GRANT SELECT ON TABLE TABLE_000 TO USER g1;
GRANT SELECT ON TABLE TABLE_000 TO USER g2;
GRANT SELECT ON TABLE TABLE_000 TO USER g3;

DROP TABLE IF EXISTS TABLE_001
CREATE TABLE TABLE_001  AS SELECT * FROM BASE_TABLE WHERE ID=001
GRANT SELECT ON TABLE TABLE_001 TO USER g4;
GRANT SELECT ON TABLE TABLE_001 TO USER g5;
GRANT SELECT ON TABLE TABLE_001 TO USER g6;

DROP TABLE IF EXISTS TABLE_002
CREATE TABLE TABLE_002  AS SELECT * FROM BASE_TABLE WHERE ID=002
GRANT SELECT ON TABLE TABLE_002 TO USER g7;
GRANT SELECT ON TABLE TABLE_002 TO USER g8;
GRANT SELECT ON TABLE TABLE_002 TO USER g9;

"""
class Segmentation:

    def process(self, cmd):
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in p.stdout.readlines():
            print line,
        retval = p.wait()   
        print(retval) 
    
    def createTable(self, sql, partition, table, serde):
        spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        df = spark.sql(sql)
        for row in df.dtypes :
            print "{0}:{1}".format(row[0],row[1])
        df.write.format(serde).partitionBy(partition).saveAsTable(table)
        
    def segment(self, data):
        print("running segmentation")
        spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        tpl = Template
        obj = json.loads(data)
        for i in obj['resources']:
            
            for j in i['segments']:                    
                spark.sql(tpl.drop_table.format(j["name"]));                        
                spark.sql(tpl.create_table.format(j["name"], i["name"], j["segment"]));                    
                for k in j['groups']:
                    if(True) :
                        print(tpl.chown.format(i["location"], i["database"], j["name"]))
                        print(tpl.chgrp.format(k, k, i["location"], i["database"], j["name"]))
                    else :
                        print(tpl.grant_selectformat(j["name"], k))
                        self.process(tpl.grant_select_beeline.format(j["name"], k))
