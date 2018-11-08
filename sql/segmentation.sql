set tez.queue.name=arabdprd_q1;
set hive.execution.engine=tez;

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