sudo su postgres
cd ~/
pg_dump dbname=noaa_test -f noaadb_test.sql
psql \
   -f noaadb_test.sql \
   --host noaadb.cfpyzdbolrva.us-west-2.rds.amazonaws.com \
   --port 5432 \
   --username postgres \
   --password *** \
   --dbname noaadb_test