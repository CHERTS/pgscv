#!/usr/bin/bash

PG_VER=17
MAIN_DATADIR=/var/lib/postgresql/data/main
STDB1_DATADIR=/var/lib/postgresql/data/standby1
STDB2_DATADIR=/var/lib/postgresql/data/standby2
LGDB1_DATADIR=/var/lib/postgresql/data/logical1

_logging() {
    local MSG=${1}
    printf "%s: %s\n" "$(date "+%d.%m.%Y %H:%M:%S")" "${MSG}" 2>/dev/null
}

# init postgres
_logging "Init main database..."
su - postgres -c "/usr/lib/postgresql/${PG_VER}/bin/initdb -k -E UTF8 --locale=en_US.UTF-8 -D ${MAIN_DATADIR}"

# add extra config parameters
_logging "Creating main postgresql.auto.conf..."
cat >> ${MAIN_DATADIR}/postgresql.auto.conf <<EOF
ssl = on
ssl_cert_file = '/etc/ssl/certs/ssl-cert-snakeoil.pem'
ssl_key_file = '/etc/ssl/private/ssl-cert-snakeoil.key'
logging_collector = on
log_directory = '/var/log/postgresql'
log_filename = 'postgresql-main.log'
track_io_timing = on
track_functions = all
shared_preload_libraries = 'pg_stat_statements'
wal_level = 'logical'
EOF

_logging "Creating pg_hba.conf..."
echo "host all pgscv 127.0.0.1/32 trust" >> ${MAIN_DATADIR}/pg_hba.conf

# run main postgres
_logging "Run main PostgreSQL v${PG_VER} via pg_ctl..."
su - postgres -c "/usr/lib/postgresql/${PG_VER}/bin/pg_ctl -w -t 30 -l /var/log/postgresql/startup-main.log -D ${MAIN_DATADIR} start"

# run standby postgres
_logging "Run pg_basebackup (standby)..."
su - postgres -c "pg_basebackup -P -R -X stream -C -S standby_test_slot -c fast -h 127.0.0.1 -p 5432 -U postgres -D ${STDB1_DATADIR}"
_logging "Creating standby 1 postgresql.auto.conf..."
cat >> ${STDB1_DATADIR}/postgresql.auto.conf <<EOF
port = 5433
log_filename = 'postgresql-standby.log'
EOF
_logging "Run standby PostgreSQL v${PG_VER} via pg_ctl..."
su - postgres -c "/usr/lib/postgresql/${PG_VER}/bin/pg_ctl -w -t 30 -l /var/log/postgresql/startup-standby.log -D ${STDB1_DATADIR} start"

# run cascade standby postgres
_logging "Run pg_basebackup (cascade standby)..."
su - postgres -c "pg_basebackup -P -R -X stream -C -S standby_test_slot_cascade -c fast -h 127.0.0.1 -p 5433 -U postgres -D ${STDB2_DATADIR}"
_logging "Creating cascade standby postgresql.auto.conf..."
cat >> ${STDB2_DATADIR}/postgresql.auto.conf <<EOF
port = 5434
log_filename = 'postgresql-cascade-standby.log'
EOF
_logging "Run cascade standby PostgreSQL v${PG_VER} via pg_ctl..."
su - postgres -c "/usr/lib/postgresql/${PG_VER}/bin/pg_ctl -w -t 30 -l /var/log/postgresql/startup-cascade-standby.log -D ${STDB2_DATADIR} start"

# add fixtures, tiny workload
_logging "Add fixtures, tiny workload..."
chown -R postgres:postgres /opt/testing
chmod 750 /opt/testing
su - postgres -c "psql -f /opt/testing/fixtures.sql"
_logging "Run pg_bench..."
su - postgres -c "pgbench -i -s 5 pgscv_fixtures"
su - postgres -c "pgbench -T 5 pgscv_fixtures"

# run logical standby postgres
_logging "Run pg_basebackup (physical standby to logical)..."
su - postgres -c "pg_basebackup -P -R -X stream -C -S standby_test_slot_physical -c fast -h 127.0.0.1 -p 5432 -U postgres -D ${LGDB1_DATADIR}"
_logging "Creating physical standby postgresql.auto.conf..."
cat >> ${LGDB1_DATADIR}/postgresql.auto.conf <<EOF
port = 5435
log_filename = 'postgresql-logical.log'
EOF
_logging "Run physical standby PostgreSQL v${PG_VER} via pg_ctl..."
su - postgres -c "/usr/lib/postgresql/${PG_VER}/bin/pg_ctl -w -t 30 -l /var/log/postgresql/startup-logical.log -D ${LGDB1_DATADIR} start"
_logging "Wait 5 second..."
sleep 5
_logging "Stop physical standby PostgreSQL v${PG_VER} via pg_ctl..."
su - postgres -c "/usr/lib/postgresql/${PG_VER}/bin/pg_ctl -D ${LGDB1_DATADIR} stop"
su - postgres -c "echo > ${LGDB1_DATADIR}/.pgpass"
chmod 600 ${LGDB1_DATADIR}/.pgpass
_logging "Run pg_createsubscriber..."
su - postgres -c "/usr/lib/postgresql/${PG_VER}/bin/pg_createsubscriber -D ${LGDB1_DATADIR} \
--publisher-server='user=postgres passfile=${LGDB1_DATADIR}/.pgpass channel_binding=disable dbname=pgscv_fixtures host=127.0.0.1 port=5432 fallback_application_name=walreceiver sslmode=disable sslnegotiation=postgres sslcompression=0 sslcertmode=disable sslsni=1 ssl_min_protocol_version=TLSv1.2 gssencmode=disable krbsrvname=postgres gssdelegation=0 target_session_attrs=any load_balance_hosts=disable' \
--database pgscv_fixtures \
--subscriber-username=postgres \
--replication-slot=pgscv_db_slot \
--publication=pgscv_db_publication \
--subscription=pgscv_db_subscription \
--verbose"
_logging "Run logical standby PostgreSQL v${PG_VER} via pg_ctl..."
su - postgres -c "/usr/lib/postgresql/${PG_VER}/bin/pg_ctl -w -t 30 -l /var/log/postgresql/startup-logical.log -D ${LGDB1_DATADIR} start"

_logging "Run pg_bench..."
su - postgres -c "pgbench -T 5 pgscv_fixtures"

# configure pgbouncer
_logging "Configure pgbouncer..."
sed -i -e 's/^;\* = host=testserver$/* = host=127.0.0.1/g' /etc/pgbouncer/pgbouncer.ini
sed -i -e 's/^;admin_users = .*$/admin_users = pgscv/g' /etc/pgbouncer/pgbouncer.ini
sed -i -e 's/^;pool_mode = session$/pool_mode = transaction/g' /etc/pgbouncer/pgbouncer.ini
sed -i -e 's/^;ignore_startup_parameters = .*$/ignore_startup_parameters = extra_float_digits/g' /etc/pgbouncer/pgbouncer.ini
echo '"pgscv" "pgscv"' > /etc/pgbouncer/userlist.txt

# run pgbouncer
_logging "Run pgbouncer..."
su - postgres -c "/usr/sbin/pgbouncer -d /etc/pgbouncer/pgbouncer.ini"

# run memcached
_logging "Run memcached..."
memcached -d -u root

# check services availability
_logging "Check services availability..."
pg_isready -t 10 -h 127.0.0.1 -p 5432 -U pgscv -d postgres
pg_isready -t 10 -h 127.0.0.1 -p 5433 -U pgscv -d postgres
pg_isready -t 10 -h 127.0.0.1 -p 5434 -U pgscv -d postgres
pg_isready -t 10 -h 127.0.0.1 -p 5435 -U pgscv -d postgres
pg_isready -t 10 -h 127.0.0.1 -p 6432 -U pgscv -d pgbouncer
