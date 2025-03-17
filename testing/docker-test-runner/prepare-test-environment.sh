#!/usr/bin/bash

PG_VER=16
MAIN_DATADIR=/var/lib/postgresql/data/main
STDB1_DATADIR=/var/lib/postgresql/data/standby1
STDB2_DATADIR=/var/lib/postgresql/data/standby2

# init postgres
echo "InitDB..."
su - postgres -c "/usr/lib/postgresql/${PG_VER}/bin/initdb -k -E UTF8 --locale=en_US.UTF-8 -D ${MAIN_DATADIR}"

# add extra config parameters
echo "Creating main postgresql.auto.conf..."
cat >> ${MAIN_DATADIR}/postgresql.auto.conf <<EOF
ssl = on
ssl_cert_file = '/etc/ssl/certs/ssl-cert-snakeoil.pem'
ssl_key_file = '/etc/ssl/private/ssl-cert-snakeoil.key'
logging_collector = on
log_directory = '/var/log/postgresql'
track_io_timing = on
track_functions = all
shared_preload_libraries = 'pg_stat_statements'
EOF

echo "Creating pg_hba.conf..."
echo "host all pgscv 127.0.0.1/32 trust" >> ${MAIN_DATADIR}/pg_hba.conf

# run main postgres
echo "Run main PostgreSQL v${PG_VER} via pg_ctl..."
su - postgres -c "/usr/lib/postgresql/${PG_VER}/bin/pg_ctl -w -t 30 -l /var/run/postgresql/startup-main.log -D ${MAIN_DATADIR} start"

# run standby 1 postgres
echo "Run pg_basebackup..."
su - postgres -c "pg_basebackup -P -R -X stream -C -S standby_test_slot -c fast -h 127.0.0.1 -p 5432 -U postgres -D ${STDB1_DATADIR}"
echo "Creating standby 1 postgresql.auto.conf..."
cat >> ${STDB1_DATADIR}/postgresql.auto.conf <<EOF
port = 5433
log_filename = 'postgresql-standby.log'
EOF
echo "Run standby PostgreSQL v${PG_VER} via pg_ctl..."
su - postgres -c "/usr/lib/postgresql/${PG_VER}/bin/pg_ctl -w -t 30 -l /var/run/postgresql/startup-standby.log -D ${STDB1_DATADIR} start"

# run cascade standby 2 postgres
echo "Run pg_basebackup..."
su - postgres -c "pg_basebackup -P -R -X stream -C -S standby_test_slot_cascade -c fast -h 127.0.0.1 -p 5433 -U postgres -D ${STDB2_DATADIR}"
echo "Creating standby 2 postgresql.auto.conf..."
cat >> ${STDB2_DATADIR}/postgresql.auto.conf <<EOF
port = 5434
log_filename = 'postgresql-standby-2.log'
EOF
echo "Run standby PostgreSQL v${PG_VER} via pg_ctl..."
su - postgres -c "/usr/lib/postgresql/${PG_VER}/bin/pg_ctl -w -t 30 -l /var/run/postgresql/startup-standby-2.log -D ${STDB2_DATADIR} start"

# add fixtures, tiny workload
echo "Add fixtures, tiny workload..."
chown -R postgres:postgres /opt/testing
chmod 750 /opt/testing
su - postgres -c "psql -f /opt/testing/fixtures.sql"
su - postgres -c "pgbench -i -s 5 pgscv_fixtures"
su - postgres -c "pgbench -T 5 pgscv_fixtures"

# configure pgbouncer
echo "Configure pgbouncer..."
sed -i -e 's/^;\* = host=testserver$/* = host=127.0.0.1/g' /etc/pgbouncer/pgbouncer.ini
sed -i -e 's/^;admin_users = .*$/admin_users = pgscv/g' /etc/pgbouncer/pgbouncer.ini
sed -i -e 's/^;pool_mode = session$/pool_mode = transaction/g' /etc/pgbouncer/pgbouncer.ini
sed -i -e 's/^;ignore_startup_parameters = .*$/ignore_startup_parameters = extra_float_digits/g' /etc/pgbouncer/pgbouncer.ini
echo '"pgscv" "pgscv"' > /etc/pgbouncer/userlist.txt

# run pgbouncer
echo "Run pgbouncer..."
su - postgres -c "/usr/sbin/pgbouncer -d /etc/pgbouncer/pgbouncer.ini"

# check services availability
echo "Check services availability..."
pg_isready -t 10 -h 127.0.0.1 -p 5432 -U pgscv -d postgres
pg_isready -t 10 -h 127.0.0.1 -p 5433 -U pgscv -d postgres
pg_isready -t 10 -h 127.0.0.1 -p 5434 -U pgscv -d postgres
pg_isready -t 10 -h 127.0.0.1 -p 6432 -U pgscv -d pgbouncer
