*****
Usage
*****

The multicorn foreign data wrapper is not different from other foreign data
wrappers.

To use it, you have to:

- Create the extension in the target database.
  As a PostgreSQL super user, run the following SQL:

  .. code-block:: sql

      CREATE EXTENSION multicorn;


- Create a server.
  In the SQL ``OPTIONS`` clause, you must provide an options named wrapper,
  containing the fully-qualified class name of the concrete python foreign data
  wrapper you want to use:

  .. code-block:: sql

      CREATE SERVER multicorn_imap FOREIGN DATA WRAPPER multicorn
      options (
        wrapper 'multicorn.imapfdw.ImapFdw'
      );


You can then proceed on with the actual foreign tables creation, and pass them
the needed options.

Each foreign data wrapper supports its own set of options, and may interpret the
columns definitions differently.

You should look at the documentation for the specific :doc:`Foreign Data Wraper documentation <foreign-data-wrappers>`
