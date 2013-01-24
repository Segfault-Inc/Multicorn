Testing
=======

To run the tests the following software is required:
- sqlalchemy
- psycopg
- PostgreSQL 9.2+
- Python 2.7+

Other requirements:
- pg_config must be in your path
- client_min_messages = notice
- trust authentication for your user. This is for the sqlalchemy connection to localhost.

To run the tests

	make installcheck
