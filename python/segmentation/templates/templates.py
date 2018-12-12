class Template:
    chown = "hadoop fs -chown {0}{1}.db/{2}"
    chgrp = "hadoop fs -chgrp {0}:{1} {2}{3}.db/{4}"
    grant_select = "GRANT SELECT ON TABLE {0} TO USER {1}; "
    grant_select_beeline ='/opt/mapr/hive/hive-2.1/bin/beeline -u "jdbc:hive2://localhost:10000/default" -n mapr -p mapr -e "set role admin; GRANT SELECT ON TABLE {0} TO USER {1}"'
    drop_table = "DROP TABLE IF EXISTS {0}"
    create_table = "CREATE TABLE {0} AS SELECT * FROM {1} WHERE ID={2}"   
    dynamic_partition_set = "hive.exec.dynamic.partition.mode=nonstrict;set hive.exec.dynamic.partition=true;"
    dynamic_partition_static = "INSERT OVERWRITE TABLE {0} PARTITION (date ='{1}') SELECT {2}, {3} FROM {4}"
    dynamic_partition_dynamic = "INSERT OVERWRITE TABLE {0} PARTITION ({1}) SELECT {2}, {1} FROM {3};"
    stats = "ANALYZE TABLE {0} COMPUTE STATISTICS FOR COLUMNS;"
    stats_partition = "ANALYZE TABLE {0} PARTITION({1}='{2}') COMPUTE STATISTICS FOR COLUMNS;"