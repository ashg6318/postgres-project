# Outline:

## How to Set Up a local instance of postgres

The basic steps to set up postgres on a local machine are given below


1. Set up Postgress on a Local Machine
	- Install postgresql using Homebrew: `brew install postgresql`
	- Install psycopg2 python package: `python3 -m pip install pyscopg2`
	- Check postgres version: `postgres --version`
	- Initiate instance: `initdb /usr/local/var/postgres`
	- Start server: `pg_ctl -D /usr/local/var/postgres -l logfile start`
	- Stop server: `pg_ctl -D /usr/local/var/postgres -l logfile stop`


2. Use psql to connect to the default database and create roles and new databases
	- Find the web resource [here!](http://postgresguide.com/utilities/psql.html)
