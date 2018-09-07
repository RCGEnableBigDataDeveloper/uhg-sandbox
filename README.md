# ARA-AllStars POC - Separation of data (filesystem and database)

Reverse engineered POC outlined in ARA-AllStars deck into new POC on local MapR cluster using same approach for separation of data (row and column level security)

## Getting Started

This POC requires an instance of MapR Coverged Platform running Hive. The steps to run the POC are listed below

### Prerequisites

Java 1.8.x, MapR 6.0.1, Hive CLI

### Installing

To reproduce the POC function, please run the following steps

Create a locations table in Hive using the supplied locations.ddl file
```
hive -f /tmp/locations.ddl
```

Load the location data from the locations.csv file
```
load data local inpath '/tmp/locations.csv' into table locations;

```

Create a  permissions table in Hive
```
create table permission( username string, uid string);
```

Insert a user profile record into the permission table
```
insert into permission values( 'root', '0' );
```

Create a view by joining the location table with the permission table on key "current_user". This will leverage the Hive current user UDF which will return the name of the user running the query 
```
create view secure_locations AS select d.* from locations d inner join permission p on d.status_code=p.uid where username = current_user(); 
```

Select status_code from the view. Only records with status code matching the users uid will be returned (d.status_code=p.uid)
```
select status_code from secure_locations limit 10;

```
Following the same approach, we can restrict access to columns
```
select CASE WHEN p.username = current_user() THEN brandname ELSE NULL END as brandname, status_code From locations t, permission p limit 10; 
```

### Using MapR ACE's to control access

Mapr Access Control Expressions provide more fine graineid control than standard POSIX Access Control Lists (ACL's). One major advantage of ACE's is the addition of "roles"
See https://mapr.com/docs/52/SecurityGuide/FileDirACE.html


### Example of using MapR ACE roles to restrict access

Add a role to the MapR ACE control file /opt/mapr/conf/m7_permissions_roles_refimpl.conf

```
test_role:
root
```

Add a file to HDFS /tmp directory

```
touch myfile && hadoop fs -put myfile /tmp
```

Apply a "read" ACE to the file 

```
hadoop mfs -setace -readfile 'r:test_role' /tmp/myfile
```

Try to read the file as a non root user
```
su user1
hadoop fs -cat /tmp/myfile
```

Invalidate the roles cache
```
/opt/mapr/server/mrconfig dbrolescache invalidate
```

You Will see an error similiar to the following
```
18/09/07 10:32:45 ERROR fs.Inode: Marking failure for: /tmp/myfile, error: Input/output error
18/09/07 10:32:45 ERROR fs.Inode: Throwing exception for: /tmp/myfile, error: Input/output error
2018-09-07 10:32:45,2695 ERROR Client fs/client/fileclient/cc/client.cc:5653 Thread: 19619 ReadRPC: /tmp/myfile, error 13, deleting fid 2050.42.393860 from cache, for off 0 node 10.0.2.15:5660
2018-09-07 10:32:45,2695 ERROR Client fs/client/fileclient/cc/client.cc:5667 Thread: 19619 Read failed for file /tmp/myfile, error Permission denied(13), off 0 len 8192 for fid 2050.42.393860
cat: 2050.42.393860 /tmp/myfile (Input/output error)
```


