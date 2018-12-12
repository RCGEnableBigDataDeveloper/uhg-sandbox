import datetime
import subprocess

from templates.templates import Template


class Partitioner():
    
    tpl = Template

    def process(self, cmd):
        p = subprocess.Popen("hive -e " + cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in p.stdout.readlines():
            print line,
        retval = p.wait()   
        print(retval) 
    
    def dynamicPartition(self, srcTable, destTable, columns, partition):
        y = columns.pop(columns.index(partition))
        print(self.tpl.dynamic_partition_set)
        self.process(self.tpl.dynamic_partition_set)
        print(self.tpl.dynamic_partition_dynamic.format(srcTable, y, ",".join(columns), destTable))
        self.process(self.tpl.dynamic_partition_dynamic.format(srcTable, y, ",".join(columns), destTable))
        
    def staticPartition(self, srcTable, destTable, columns, partition):
        now = datetime.datetime.now()
        columns.pop(columns.index(partition))
        print(self.tpl.dynamic_partition_set)
        self.process(self.tpl.dynamic_partition_set)
        print(self.tpl.dynamic_partition_static.format("TABLE1", "{0}-{1}-{2}".format(now.day, now.month, now.year), ",".join(columns), "date", "TABLE2"))
        self.process(self.tpl.dynamic_partition_static.format("TABLE1", "{0}-{1}-{2}".format(now.day, now.month, now.year), ",".join(columns), "date", "TABLE2"))
    
if __name__ == '__main__':
    p = Partitioner()
    columns =  ['id', 'name', "address", "date"]
    p.dynamicPartition("SRC_TABLE","DEST_TABLE",columns,"date")
    
    columns = ['id', 'name', "address", "date"]
    p.staticPartition("SRC_TABLE","DEST_TABLE",columns,"date")
