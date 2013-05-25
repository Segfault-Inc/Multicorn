SyapseFDW
=========

Synopsis
--------

Syapse (syapse.com) is an RDF-based data store for semi-structured data.
SyapseFDW provides a PostgreSQL foreign data wrapper to Syapse.
 
The Syapse Python-based API provide one-at-a-time record lookup.  It does
not support querying.  As a result, SyapseFDW is fairly slow (apx 1
second/row for small rows).

The current code obeys Syapse cardinality: properties with cardinality AtMostOne or ExactlyOne are returned as values or None; properties with cardinality are returned as lists (potentially empty).


Installation
------------

Install multicorn per those instructions. ('make install' is sufficient for me.)


Then, in psql:

    DROP FOREIGN TABLE syapse;
    DROP SERVER syapse;
 
    CREATE SERVER syapse
    FOREIGN DATA WRAPPER multicorn options (
        wrapper  'multicorn.syapsefdw.SyapseFDW'
    );
    
    CREATE FOREIGN TABLE locus_patient (
        description               TEXT        -- AtMostOne ; HtmlText  ; description
      , has_locus_experiment      TEXT        -- Any       ; ivl:LocusExperiment; hasLocusExperiment
      , locus_gender              TEXT        -- AtMostOne ; String    ; locusGender
      , assigned_project          TEXT        -- Any       ; None      ; assignedProject
      , locus_patient_id          TEXT        -- AtMostOne ; String    ; locusPatientId
      , locus_ethnicity           TEXT        -- AtMostOne ; String    ; locusEthnicity
      , unique_id                 TEXT        -- AtMostOne ; String    ; uniqueId
      , date_created              TEXT        -- ExactlyOne; Datetime  ; date_created
      , owner                     TEXT        -- AtMostOne ; None      ; owner
      , date_changed              TEXT        -- ExactlyOne; Datetime  ; date_changed
      , has_locus_patient_relation TEXT        -- Any       ; ivl:LocusPatientRelation; hasLocusPatientRelation
      , name                      TEXT        -- ExactlyOne; String    ; name
    ) server syapse options (
      syapse_hostname '...',
      syapse_email    '...',
      syapse_password '...',
      syapse_class	  'LocusPatient'
    );

    reece=# select unique_id,name,locus_gender,locus_ethnicity,date_created
       from locus_patient limit 2;
     unique_id |  name   | locus_gender | locus_ethnicity |    date_created     
    -----------+---------+--------------+-----------------+---------------------
     IP2000    | EX014-1 | Female       | Caucasian       | 2013-05-23T11:44:03
     IP1757    | EX009-4 | Female       | Caucasian       | 2013-05-16T09:28:43
    (2 rows)
    


ToDo
----
* use unique_id predicates if available
* expose AppIndividualId, and use these predicates if available
* support saved queries, which are much faster but require downloading all data
* write CREATE TABLE generator
