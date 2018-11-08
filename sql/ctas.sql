drop table if exists ara.employer_all;

CREATE TABLE
ara.employer_all as
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
ara.employer e,
ara.employer_address a
WHERE
0=1 limit 1;

alter table p1 add partition (state_cd)

insert overwrite table ara.employer_all partition(state_cd)
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
ara.employer e,
ara.employer_address a
WHERE
0=1;



SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
INSERT OVERWRITE TABLE ara.employer_all PARTITION(Date) select date from ara.employer;
