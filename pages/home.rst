.. contents::

Presentation
============

Multicorn is a PostgreSQL 9.1+ extension meant to make `Foreign Data Wrapper`_
development easy, by allowing the programmer to use the Python programming
language.

Ok, but why should I care ?
---------------------------

- Multicorn allows you to access any data source in your PostgreSQL database.
- You can leverage the full power of SQL to query your data sources
- Every tool you use for SQL can be reused with those datasources (think about
  an ORM, BI tool...)

Installation
============

With the `pgxn client`_::

   pgxn install multicorn --testing

From pgxn::

   wget http://api.pgxn.org/dist/multicorn/0.0.5/multicorn-0.0.5.zip ./
   unzip multicorn-0.0.5.zip
   cd multicorn-0.0.5/
   make && sudo make install

From source::

    git clone git://github.com/Kozea/Multicorn.git
    cd Multicorn
    make && make install

.. _Foreign Data Wrapper: http://people.planetpostgresql.org/andrew/uploads/fdw2.pdf
.. _pgxn client: http://pgxnclient.projects.postgresql.org/


Usage
=====

The multicorn foreign data wrapper is not different from other foreign data
wrappers.

To use it, you have to:

- Create the extension in the target database.
  As a PostgreSQL super user, run the following SQL:

  .. code-block:: sql

      CREATE EXTENSION multicorn;


- Create a "foreign server".
  In the SQL ``OPTIONS`` clause, you must provide an options named wrapper,
  containing the fully-qualified class name of the concrete python foreign data
  wrapper you wish to use. want to use:

  .. code-block:: sql

      CREATE SERVER multicorn_imap FOREIGN DATA WRAPPER multicorn
      options (
        wrapper 'multicorn.imapfdw.ImapFdw'
      );


You can then proceed on with the actual foreign tables creation, and pass them
the needed options

For example, if you want to use the imap foreign data wrapper, you can define
it like this:

.. code-block:: sql

    create foreign table gmail (                                                                 
        "Message-ID" character varying,
        "From" character varying,
        "Subject" character varying,
        "payload" character varying,
        "flags" character varying[],
        "To" character varying) server multicorn_imap options (
            host 'imap.gmail.com',
            port '465', 
            payload_column 'payload', 
            flags_column 'flags',
            ssl 'True',
            login 'mylogin', 
            password 'mypassword'
    );

    select flags, "Subject", payload  from gmail where "From" like 'pgsql-%'
    and "Subject" like '%Daily digest%' limit 2;

.. code-block:: bash


     flags  |                       Subject                       |                                         payload                                          
    --------+-----------------------------------------------------+------------------------------------------------------------------------------------------
     {Seen} | [pgsql-hackers] Daily digest v1.13135 (18 messages) | Message Digest \r                                                                       +
            |                                                     | Volume 1 : Issue 13135 : "index" Format\r                                               +
            |                                                     | \r                                                                                      +
            |                                                     | Messages in this Issue:\r                                                               +
            |                                                     |   201110/1145: Re: [v9.2] DROP statement reworks\r                                      +
            |                                                     |   201110/1146: Re: EXECUTE tab completion \r                                            +
            |                                                     |   201110/1147: Re: Update on documentation builds on OSX w/ macports\r                  +
            |                                                     |   201110/1148: Re: EXECUTE tab completion\r                                             +
            |                                                     |   201110/1149: Re: SSI implementation question\r                                        +
            |                                                     |   201110/1150: Re: ProcessStandbyHSFeedbackMessage can make global xmin go backwards \r +
            |                                                     |   201110/1151: Re: [v9.2] make_greater_string() does not return a\r                     +
            |                                                     |  string in some cases\r                                                                 +
            |                                                     |   201110/1152: Re: loss of transactions in streaming replication\r                      +
            |                                                     |   201110/1153: Re: loss of transactions in streaming replication\r                      +
            |                                                     |   201110/1154: Re: [v9.2] DROP statement reworks\r                                      +
            |                                                     |   201110/1155: Re: funny lock mode in DropTrigger\r                                     +
            |                                                     |   201110/1156: funny lock mode in DropTrigger\r                                         +
            |                                                     |   201110/1157: Re: ProcessStandbyHSFeedbackMessage can make global xmin go backwards\r  +
            |                                                     |   201110/1158: psql \set vs \copy - bug or expected behaviour?\r                        +
            |                                                     |   201110/1159: Re: [v9.2] DROP statement reworks\r                                      +
            |                                                     |   201110/1160: Re: [v9.2] Fix Leaky View Problem\r                                      +
            |                                                     |   201110/1161: Re: pg_upgrade if 'postgres' database is dropped\r                       +
            |                                                     |   201110/1162: Re: [v9.2] Fix Leaky View Problem\r                                      +
            |                                                     | 
     {Seen} | [pgsql-hackers] Daily digest v1.13136 (15 messages) | Message Digest \r                                                                       +
            |                                                     | Volume 1 : Issue 13136 : "index" Format\r                                               +
            |                                                     | \r                                                                                      +
            |                                                     | Messages in this Issue:\r                                                               +
            |                                                     |   201110/1163: Re: pg_dumpall Sets Roll default_tablespace Before Creating Tablespaces\r+
            |                                                     |   201110/1164: Re: WIP: Join push-down for foreign tables\r                             +
            |                                                     |   201110/1165: Synchronized snapshots versus multiple databases\r                       +
            |                                                     |   201110/1166: Re: [PATCH] Log crashed backend's query v3\r                             +
            |                                                     |   201110/1167: Re: ProcessStandbyHSFeedbackMessage can make global xmin go backwards\r  +
            |                                                     |   201110/1168: Re: pg_comments (was: Allow \dd to show constraint comments)\r           +
            |                                                     |   201110/1169: Re: ProcessStandbyHSFeedbackMessage can make global xmin go backwards \r +
            |                                                     |   201110/1170: Re: Synchronized snapshots versus multiple databases\r                   +
            |                                                     |   201110/1171: Re: funny lock mode in DropTrigger \r                                    +
            |                                                     |   201110/1172: Re: funny lock mode in DropTrigger \r                                    +
            |                                                     |   201110/1173: Re: Synchronized snapshots versus multiple databases\r                   +
            |                                                     |   201110/1174: So, is COUNT(*) fast now?\r                                              +
            |                                                     |   201110/1175: Re: [v9.2] Object access hooks with arguments support (v1)\r             +
            |                                                     |   201110/1176: Re: [v9.2] Object access hooks with arguments support (v1)\r             +
            |                                                     |   201110/1177: Re: Synchronized snapshots versus multiple databases \r                  +
            |                                                     | 



Each foreign data wrapper supports its own set of options, and may interpret the
columns definitions differently.

You should take a look to the `documentation </documentation/>`_.
