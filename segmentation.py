import sys
import json
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
import subprocess

""" Generate segmentation tables

This module demonstrates how to automate segmentation of data and applying
authorization using Hive SQL Standard Authoriaztion.

Example:
   The following json structure...

    {
        "type": "auth",
        "resources": [{
            "name": "test",
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

def process(cmd):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in p.stdout.readlines():
        print line,
    retval = p.wait()    

    
if __name__ == '__main__':
    
        spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        segments = ["000", "001", "002"]
        data = ('''{
            "type": "auth",
            "resources": [{
                "name": "test",
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
        }''')
        
        obj = json.loads(data)

        print(obj["type"])
        for i in obj['resources']:
            print i['name']
            for j in i['segments']:                    
                spark.sql("DROP TABLE IF EXISTS {0}".format(j["name"]));                        
                spark.sql("CREATE TABLE {0}  AS SELECT * FROM {1} WHERE ID={2}".format(j["name"], i["name"], j["segment"]));                    
                for k in j['groups']:
                    print("GRANT SELECT ON TABLE {0} TO USER {1}; ".format(j["name"], k))
                    process('/opt/mapr/hive/hive-2.1/bin/beeline -u "jdbc:hive2://localhost:10000/default" -n mapr -p mapr -e "set role admin; GRANT SELECT ON TABLE {0} TO USER {1}"'.format(j["name"], k))
        
        sys.exit(0)

