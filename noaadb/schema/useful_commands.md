# Migrating

### dump:

##### whole database:
pg_dump -a -F p  -U postgres noaa -f remote_copy.pgsql

##### schema + data -
pg_dump -n schema_name -F p -U postgres -d database_name -f /var/lib/postgresql/dumps/4-22-2020-chips-schema.dump

### restore
psql -U postgres database_name < 4-22-2020-chips-schema.dump