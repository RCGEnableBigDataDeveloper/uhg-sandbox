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
load data local inpath '/tmp/retail_locations.csv' into table locations;

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



End with an example of getting some data out of the system or using it for a little demo

## Running the tests

Explain how to run the automated tests for this system

### Break down into end to end tests

Explain what these tests test and why

```
Give an example
```

### And coding style tests

Explain what these tests test and why

```
Give an example
```

## Deployment

Add additional notes about how to deploy this on a live system

## Built With

* [Dropwizard](http://www.dropwizard.io/1.0.2/docs/) - The web framework used
* [Maven](https://maven.apache.org/) - Dependency Management
* [ROME](https://rometools.github.io/rome/) - Used to generate RSS Feeds

## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags). 

## Authors

* **Billie Thompson** - *Initial work* - [PurpleBooth](https://github.com/PurpleBooth)

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc

