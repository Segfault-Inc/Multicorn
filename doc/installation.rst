************
Installation
************

Requirements
============

- Postgresql 9.1+
- Postgresql development packages
- Python development packages
- python 2.6 or >= python 3.3 as your default python

If you are using *PostgreSQL 9.1*, you should use the 0.9.1 release.

If you are using *PostgreSQL 9.2* or superior, you should use the 1.0.0  series. (Currently
1.0.1).

If you are using Debian, a packaging effort is ongoing for PostgreSQL 9.4. You can install it from
`here <https://packages.debian.org/unstable/database/postgresql-9.4-python-multicorn>`_.


With the `pgxn client`_::

   pgxn install multicorn

From pgxn:

.. parsed-literal::

   wget |multicorn_pgxn_download|
   unzip multicorn-|multicorn_release|
   cd multicorn-|multicorn_release|
   make && sudo make install

From source::

    git clone git://github.com/Kozea/Multicorn.git
    cd Multicorn
    make && make install

.. _pgxn client: http://pgxnclient.projects.postgresql.org/

