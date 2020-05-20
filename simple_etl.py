# Program Name: pyEL (EL: Extract & Load)

# import  libraries
import petl as etl
import psycopg2 as pg
import sys
from sqlalchemy import *

# declare connection properties within dictionary
dbCnxns = {'pagila': 'dbname=pagila user=postgres host=127.0.0.1',
           'pagila_dw': 'dbname=pagila_dw user=postgres host=127.0.0.1'}

# set connections and cursors
# grab value by referencing key dictionary
sourceConn = pg.connect(dbCnxns['pagila'])
# grab value by referencing key dictionary
targetConn = pg.connect(dbCnxns['pagila_dw'])
sourceCursor = sourceConn.cursor()
targetCursor = targetConn.cursor()

# retrieve the names of the source tables to be copied
sourceCursor.execute("""select table_name from information_schema.tables
where table_schema = 'public'
and table_type = 'BASE TABLE'
and table_name in ('film', 'actor')""")
sourceTables = sourceCursor.fetchall()

# iterate through table names to copy over
for t in sourceTables:
    targetCursor.execute("drop table if exists %s" % (t[0]))
    sourceDs = etl.fromdb(sourceConn, "select * from %s" % (t[0]))
    etl.todb(sourceDs, targetConn, t[0], create=True, sample=10000)
