Installation
============

Requirements
------------

- Postgresql 9.1+
- Postgresql development packages
- Python development packages
- python 2.7 or >= python 3.3 as your default python

If you are using *PostgreSQL 9.1*, you should use the 0.9.1 release.

If you are using *PostgreSQL 9.2* or superior, you should use the 1.0.0  series. (Currently
1.0.1).


With the `pgxn client`_::

   pgxn install multicorn

From pgxn::

   wget http://api.pgxn.org/dist/multicorn/1.0.1-beta1/multicorn-1.0.1-beta1.zip
   unzip multicorn-1.0.1.zip
   cd multicorn-1.0.1/
   make && sudo make install

From source::

    git clone git://github.com/Kozea/Multicorn.git
    cd Multicorn
    make && make install

.. _pgxn client: http://pgxnclient.projects.postgresql.org/

