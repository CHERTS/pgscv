#!/bin/bash

# Don't edit this config
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do
	DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
	SOURCE="$(readlink "$SOURCE")"
	[[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
done
SCRIPT_DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
SCRIPT_NAME=$(basename "$0")

PG_DATADIR=${PGDATA:-"/data/postgres"}
PG_HOST=${PG_REPLICA_HOST:-"postgres1"}
PG_PORT=${PG_REPLICA_PORT:-"5432"}
PG_REPLUSER=${PG_REPL_USER:-"repluser"}
PG_REPLUSER_PASSWORD=${PG_REPL_PASSWORD:-"repluser"}
PG_REPL_SLOT=${PG_REPL_SLOT:-"replica_slot1"}

DATE_START=$(date +"%s")

# Logging function
_logging() {
	local MSG=${1}
	local ENDLINE=${2:-"1"}
	if [[ "${ENDLINE}" -eq 0 ]]; then
		printf "%s: %s" "$(date "+%d.%m.%Y %H:%M:%S")" "${MSG}" 2>/dev/null
	else
		printf "%s: %s\n" "$(date "+%d.%m.%Y %H:%M:%S")" "${MSG}" 2>/dev/null
	fi
}

# Calculate duration function
_duration() {
	local DATE_START=${1:-"$(date +'%s')"}
	local FUNC_NAME=${2:-""}
	local DATE_END=$(date +"%s")
	local D_MSG=""
	local DATE_DIFF=$((${DATE_END} - ${DATE_START}))
	if [ -n "${FUNC_NAME}" ]; then
		local D_MSG=" of execute function '${FUNC_NAME}'"
	fi
	_logging "Duration${D_MSG}: $((${DATE_DIFF} / 3600)) hours $(((${DATE_DIFF} % 3600) / 60)) minutes $((${DATE_DIFF} % 60)) seconds"
}

_logging "Starting script ${SCRIPT_DIR}/${SCRIPT_NAME}"

_logging "Script options:"
_logging "PGDATA: ${PG_DATADIR}"
_logging "PG_HOST: ${PG_HOST}"
_logging "PG_PORT: ${PG_PORT}"
_logging "PG_REPLUSER: ${PG_REPLUSER}"
_logging "PG_REPLUSER_PASSWORD: *****"
_logging "PG_REPL_SLOT: ${PG_REPL_SLOT}"

PG_MAJOR_VER=$(pg_config --version 2>/dev/null | awk -F' ' '{print $2}' | awk -F'.' '{print $1}')

_logging "PostgreSQL major version: ${PG_MAJOR_VER}"

if [ "${PG_MAJOR_VER}" -le 9 ]; then
    PG_BASEBACKUP_OPTS="--verbose --progress --write-recovery-conf --xlog-method=stream --checkpoint=fast"
elif [ "${PG_MAJOR_VER}" -eq 10 ]; then
    PG_BASEBACKUP_OPTS="--verbose --progress --write-recovery-conf --wal-method=stream --checkpoint=fast"
else
    PG_BASEBACKUP_OPTS="--verbose --progress --write-recovery-conf --wal-method=stream --checkpoint=fast --create-slot"
fi

if [ ! -f "${PG_DATADIR}/backup_label.old" ]; then
    _logging "Shutting down PostgreSQL v${PG_MAJOR_VER}..."
    pg_ctl stop -D ${PG_DATADIR} -m fast
    _logging "Remove old data..."
    shopt -s dotglob
    rm -rf ${PG_DATADIR}/* >/dev/null 2>&1
    _logging "Waiting for PostgreSQL to start on server ${PG_HOST}:${PG_PORT}..."
    while ! PGPASSWORD=${PG_REPLUSER_PASSWORD} pg_isready -h ${PG_HOST} -p ${PG_PORT} -d postgres -U ${PG_REPLUSER} -t 5 &> /dev/null; do 
        sleep 0.5
        _logging "Waiting for PostgreSQL to start..."
    done
    if [ "${PG_MAJOR_VER}" -le 10 ]; then
        _logging "Creating replication slot..."
        PGPASSWORD=${PG_REPLUSER_PASSWORD} psql --host=${PG_HOST} --port=${PG_PORT} --username=${PG_REPLUSER} --dbname=postgres --command="SELECT pg_create_physical_replication_slot('${PG_REPL_SLOT}');"
        if [ $? -ne 0 ]; then
            _logging "Failed to create replication slot, remove old data and exit..."
            shopt -s dotglob
            rm -rf ${PG_DATADIR}/* >/dev/null 2>&1
            exit 1
        fi
    fi
    _logging "Run pg_basebackup with options: ${PG_BASEBACKUP_OPTS}..."
    PGPASSWORD=${PG_REPLUSER_PASSWORD} pg_basebackup ${PG_BASEBACKUP_OPTS} --host=${PG_HOST} --port=${PG_PORT} --username=${PG_REPLUSER} --pgdata=${PG_DATADIR} --slot=${PG_REPL_SLOT}
    if [ $? -eq 0 ]; then
        _logging "pg_basebackup done."
        _duration "${DATE_START}"
    else
        _logging "Remove replication slot..."
        PGPASSWORD=${PG_REPLUSER_PASSWORD} psql --host=${PG_HOST} --port=${PG_PORT} --username=${PG_REPLUSER} --dbname=postgres --command="SELECT pg_drop_replication_slot('${PG_REPL_SLOT}');"
        _logging "Failed to create backup, remove old data and exit..."
        shopt -s dotglob
        rm -rf ${PG_DATADIR}/* >/dev/null 2>&1
        exit 1
    fi
fi

if [ -d "${PG_DATADIR}" ]; then
    _logging "Change owner..."
    chown -R postgres:postgres "${PG_DATADIR}" >/dev/null 2>&1
    _logging "Set permitions..."
    chmod 0700 "${PG_DATADIR}" >/dev/null 2>&1
fi

if [ ! -f "${PG_DATADIR}/backup_label.old" ]; then
    _logging "Starting PostgreSQL v${PG_MAJOR_VER}..."
    pg_ctl start -D ${PG_DATADIR}
    sleep 2
    _logging "Shutting down PostgreSQL v${PG_MAJOR_VER}..."
    pg_ctl stop -D ${PG_DATADIR} -m fast
fi

_logging "End script ${SCRIPT_DIR}/${SCRIPT_NAME}"