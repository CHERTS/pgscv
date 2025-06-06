#!/bin/bash

# postgres OR pgbouncer
PG_HOST="pgbouncer"
# default port
PG_PORT=5432
# stop only this pgbench
# ALL - stop all versions of pgbench using PG_VERSIONS array
# PATRONI - stop pgbench for patroni
# 9 - stop only pgbench for postgres v9
# ...
# 17 - stop only pgbench for postgres v17
PG_BENCH_VERSION_RUN=${1:-"ALL"}

# Don't edit this config
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do
	DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
	SOURCE="$(readlink "$SOURCE")"
	[[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
done
SCRIPT_DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
SCRIPT_NAME=$(basename "$0")

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

PG_VERSIONS=(
	"9,1.4.5"
	"10,1.4.5"
	"11,1.4.5"
	"12,1.4.5"
	"13,1.4.6"
	"14,1.4.7"
	"15,1.4.8"
	"16,1.5.0"
	"17,1.5.0"
)

_logging "Starting script."

PG_BENCHES_RUN=()
if [[ "${PG_BENCH_VERSION_RUN}" == "ALL" ]]; then
	PG_BENCHES_RUN=(${PG_VERSIONS[@]})
	RUN_PGBENCH_PATRONI=1
	_logging "Selected to stop for all versions of pgbench."
elif [[ "${PG_BENCH_VERSION_RUN}" == "PATRONI" ]]; then
	_logging "Selected to stop pgbench for patroni"
	RUN_PGBENCH_PATRONI=1
else
	for DATA in ${PG_VERSIONS[@]}; do
		PG_VER=$(echo "${DATA}" | awk -F',' '{print $1}')
		PGREPACK_VER=$(echo "${DATA}" | awk -F',' '{print $2}')
		if [[ "${PG_BENCH_VERSION_RUN}" == "${PG_VER}" ]]; then
			PG_BENCHES_RUN=(${PG_VER},${PGREPACK_VER})
			break
		fi
	done
	_logging "Selected to stop pgbench version v${PG_VER}"
fi

for DATA in ${PG_BENCHES_RUN[@]}; do
	PG_VER=$(echo "${DATA}" | awk -F',' '{print $1}')
	PGREPACK_VER=$(echo "${DATA}" | awk -F',' '{print $2}')
	STOP_FILE="${SCRIPT_DIR}/pgbench/stop_pgbench_${PG_HOST}${PG_VER}_${PG_PORT}"
	_logging "Creating stop-file '${STOP_FILE}'"
	touch "${STOP_FILE}" >/dev/null 2>&1
done

if [[ ${RUN_PGBENCH_PATRONI} -eq 1 ]]; then
	STOP_FILE="${SCRIPT_DIR}/pgbench/stop_pgbench_haproxy_5000"
	_logging "Creating stop-file '${STOP_FILE}'"
	touch "${STOP_FILE}" >/dev/null 2>&1
	STOP_FILE="${SCRIPT_DIR}/pgbench/stop_pgbench_haproxy_5001"
	_logging "Creating stop-file '${STOP_FILE}'"
	touch "${STOP_FILE}" >/dev/null 2>&1
fi

_logging "Wait for all containers running pgbench to complete, this may take up to 2 minutes."
_duration "${DATE_START}"

_logging "End script. Goodbye ;)"
