import subprocess

from templates.templates import Template

class Analyzer():

    tpl = Template

    def process(self, cmd):
        p = subprocess.Popen("hive -e " + cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in p.stdout.readlines():
            print line,
        retval = p.wait()   
        print(retval) 
    
    def analyze(self, table):
        print(self.tpl.stats.format(table))
        
    def analyzePartition(self, table, partition, value):
        print(self.tpl.stats_partition.format(table, partition, value))        
    
if __name__ == '__main__':
    a =Analyzer()
    a.analyze("TABLE")
    a.analyzePartition("TABLE", "date", "12-12-2018")
