sudo su postgres
cd ~/
pg_dump -h pepdb.c1twklssgqoz.us-west-2.rds.amazonaws.com -p 5432 -U postgres dbname=noaadb -f backup.sql
psql \
   -f backup.sql \
   --host noaadb.xxx.xxx.rds.amazonaws.com \
   --port 5432 \
   --username postgres \
   --password *** \
   --dbname noaadb_test