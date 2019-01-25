import sys
import subprocess
import time
import multiprocessing
from multiprocessing import Pool
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from templates.templates import Template

def unwrap_self_f(arg, **kwarg):
    return C.f(*arg, **kwarg)
 
class C:

    def run_cmd(self, args_list):
        print('Running system command: {0}'.format(' '.join(args_list)))
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_output, s_err = proc.communicate()
        s_return =  proc.returncode
        return s_return, s_output, s_err 

    def f(self, name):
        print multiprocessing.current_process()
        qry = "SELECT * FROM UDL_CDB.INFO_FULL WHERE PRFL_ID={0} LIMIT 1".format(name)
        #self.process('/usr/bin/hive -e "{0}"'.format(qry))
        (ret, out, err)= self.run_cmd(['/usr/bin/hive', '-e', '"set hive.execution.engine=tez;set tez.queue.name=arabdprd_q1;DROP TABLE IF EXISTS UDL_CDB.INFO_FULL_{0};CREATE TABLE UDL_CDB.INFO_FULL_{0} AS SELECT * FROM UDL_CDB.INFO_FULL WHERE PRFL_ID={0} LIMIT 10"'.format(name)])
        lines = out.split('\n')
        print(lines)
     
    def run(self):
        pool = Pool(processes=2)
        print("starting spark session")
        spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        df = spark.sql("SELECT DISTINCT PRFL_ID FROM UDL_CDB.INFO_FULL")
        #df = spark.sql("SELECT 0 AS PRFL_ID FROM UDL_CDB.INFO_FULL LIMIT 1")
        segments = df.rdd.map(lambda p: p.PRFL_ID).collect()
        for name in segments:
            print(name)

        pool.map(unwrap_self_f, zip([self]*len(segments), segments))
 
if __name__ == '__main__':
    c = C()
    c.run()
