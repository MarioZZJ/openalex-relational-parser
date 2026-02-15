#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<"USAGE"
Run OpenAlex parsing with one process per entity and merge outputs.

Usage:
  scripts/run_parallel_entities.sh --snapshot PATH --output-dir PATH [options]

Required:
  --snapshot PATH           OpenAlex snapshot root (contains works/, authors/, ...)
  --output-dir PATH         Final merged CSV output directory

Optional:
  --schema PATH             CWTS schema SQL path (default: data/reference/openalex_cwts_schema.sql)
  --reference-dir PATH      Reference ID directory (default: <output-dir>/reference_ids)
  --python-bin BIN          Python executable (default: python)
  --py-path PATH            PYTHONPATH prefix (default: src)
  --updated-date YYYY-MM-DD Restrict to updated_date partition (repeatable)
  --max-files N             Limit gzip files per entity (for smoke tests)
  --max-records N           Limit records per entity (for smoke tests)
  --progress-interval N     Records between progress logs
  --skip-merged-ids         Skip IDs listed in snapshot merged_ids
  --fail-fast               Stop all entity jobs as soon as one fails
  --keep-temp               Keep temporary per-entity outputs and logs
  -h, --help                Show this help text
USAGE
}

ENTITIES=(
  works
  authors
  institutions
  concepts
  domains
  fields
  subfields
  topics
  funders
  publishers
  sources
)

REFERENCE_TABLE_FILES=(
  work_type.csv
  doi_registration_agency.csv
  oa_status.csv
  apc_provenance.csv
  fulltext_origin.csv
  data_source.csv
  version.csv
  license.csv
  author_position.csv
  sustainable_development_goal.csv
  institution_type.csv
  institution_relationship_type.csv
  region.csv
  source_type.csv
)

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

SNAPSHOT=""
OUTPUT_DIR=""
SCHEMA="${REPO_ROOT}/data/reference/openalex_cwts_schema.sql"
REFERENCE_DIR=""
PYTHON_BIN="python"
PY_PATH="${REPO_ROOT}/src"

SKIP_MERGED_IDS=0
KEEP_TEMP=0
FAIL_FAST=0
MAX_FILES=""
MAX_RECORDS=""
PROGRESS_INTERVAL=""
UPDATED_DATES=()
LIVE_REFRESH_SECONDS=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --snapshot)
      SNAPSHOT="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --schema)
      SCHEMA="$2"
      shift 2
      ;;
    --reference-dir)
      REFERENCE_DIR="$2"
      shift 2
      ;;
    --python-bin)
      PYTHON_BIN="$2"
      shift 2
      ;;
    --py-path)
      PY_PATH="$2"
      shift 2
      ;;
    --updated-date)
      UPDATED_DATES+=("$2")
      shift 2
      ;;
    --max-files)
      MAX_FILES="$2"
      shift 2
      ;;
    --max-records)
      MAX_RECORDS="$2"
      shift 2
      ;;
    --progress-interval)
      PROGRESS_INTERVAL="$2"
      shift 2
      ;;
    --skip-merged-ids)
      SKIP_MERGED_IDS=1
      shift
      ;;
    --fail-fast)
      FAIL_FAST=1
      shift
      ;;
    --keep-temp)
      KEEP_TEMP=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "${SNAPSHOT}" || -z "${OUTPUT_DIR}" ]]; then
  echo "--snapshot and --output-dir are required." >&2
  usage >&2
  exit 1
fi

if [[ -z "${REFERENCE_DIR}" ]]; then
  REFERENCE_DIR="${OUTPUT_DIR}/reference_ids"
fi

if [[ ! -d "${SNAPSHOT}" ]]; then
  echo "Snapshot directory not found: ${SNAPSHOT}" >&2
  exit 1
fi

if [[ ! -f "${SCHEMA}" ]]; then
  echo "Schema SQL file not found: ${SCHEMA}" >&2
  exit 1
fi

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "Python executable not found: ${PYTHON_BIN}" >&2
  exit 1
fi

mkdir -p "${OUTPUT_DIR}" "${REFERENCE_DIR}"

TMP_ROOT="${OUTPUT_DIR}/_parallel_tmp"
ENTITY_OUTPUT_ROOT="${TMP_ROOT}/entities"
LOG_DIR="${TMP_ROOT}/logs"
COLLECT_OUT_DIR="${TMP_ROOT}/collect"

mkdir -p "${ENTITY_OUTPUT_ROOT}" "${LOG_DIR}" "${COLLECT_OUT_DIR}"

cleanup() {
  if [[ ${KEEP_TEMP} -eq 0 ]]; then
    rm -rf "${TMP_ROOT}"
  fi
}
trap cleanup EXIT

run_cli() {
  local log_file="$1"
  shift
  PYTHONPATH="${PY_PATH}${PYTHONPATH:+:${PYTHONPATH}}" "${PYTHON_BIN}" -m openalex_parser.cli "$@" >"${log_file}" 2>&1
}

is_reference_table_file() {
  local file_name="$1"
  local candidate
  for candidate in "${REFERENCE_TABLE_FILES[@]}"; do
    if [[ "${candidate}" == "${file_name}" ]]; then
      return 0
    fi
  done
  return 1
}

declare -A LOG_CURSOR=()
LAST_LIVE_TOKEN=""

monitor_latest_line_global() {
  local candidate_entity=""
  local candidate_line=""
  local candidate_line_no=0
  local candidate_mtime=-1
  local entity

  for entity in "${ENTITIES[@]}"; do
    local log_file="${LOG_DIR}/${entity}.log"
    [[ -f "${log_file}" ]] || continue

    local line_count
    line_count=$(wc -l < "${log_file}" 2>/dev/null || echo 0)
    local previous_count="${LOG_CURSOR[${entity}]:-0}"

    if (( line_count <= previous_count )); then
      continue
    fi

    LOG_CURSOR["${entity}"]=${line_count}

    local line
    line=$(tail -n 1 "${log_file}" 2>/dev/null || true)
    if [[ -z "${line}" ]]; then
      continue
    fi

    local modified_epoch
    modified_epoch=$(stat -c %Y "${log_file}" 2>/dev/null || echo 0)
    if (( modified_epoch >= candidate_mtime )); then
      candidate_mtime=${modified_epoch}
      candidate_entity="${entity}"
      candidate_line="${line}"
      candidate_line_no=${line_count}
    fi
  done

  if [[ -n "${candidate_entity}" ]]; then
    local token="${candidate_entity}:${candidate_line_no}"
    if [[ "${token}" != "${LAST_LIVE_TOKEN}" ]]; then
      LAST_LIVE_TOKEN="${token}"
      printf '[live][%s][%s] %s\n' "$(date +%H:%M:%S)" "${candidate_entity}" "${candidate_line}"
    fi
  fi
}

declare -a COMMON_ARGS
COMMON_ARGS=(
  --schema "${SCHEMA}"
  --snapshot "${SNAPSHOT}"
  --reference-dir "${REFERENCE_DIR}"
)

if [[ -n "${MAX_FILES}" ]]; then
  COMMON_ARGS+=(--max-files "${MAX_FILES}")
fi

if [[ -n "${MAX_RECORDS}" ]]; then
  COMMON_ARGS+=(--max-records "${MAX_RECORDS}")
fi

if [[ -n "${PROGRESS_INTERVAL}" ]]; then
  COMMON_ARGS+=(--progress-interval "${PROGRESS_INTERVAL}")
fi

if [[ ${SKIP_MERGED_IDS} -eq 1 ]]; then
  COMMON_ARGS+=(--skip-merged-ids)
fi

if [[ ${#UPDATED_DATES[@]} -gt 0 ]]; then
  for updated_date in "${UPDATED_DATES[@]}"; do
    COMMON_ARGS+=(--updated-date "${updated_date}")
  done
fi

start_time=$(date +%s)

echo "[1/3] Collecting reference IDs..."
collect_start=$(date +%s)
if ! run_cli "${LOG_DIR}/collect.log" --entity all --output-dir "${COLLECT_OUT_DIR}" --collect-only "${COMMON_ARGS[@]}"; then
  echo "Reference ID collection failed. See ${LOG_DIR}/collect.log" >&2
  tail -n 40 "${LOG_DIR}/collect.log" >&2 || true
  exit 1
fi
collect_end=$(date +%s)
echo "[1/3] Done in $((collect_end - collect_start))s"

echo "[2/3] Parsing entities in parallel (${#ENTITIES[@]} processes)..."
parse_start=$(date +%s)

declare -A ENTITY_PIDS=()
declare -A ENTITY_RCS=()
declare -A ENTITY_DONE=()
FAILED_ENTITIES=()

add_failure() {
  local entity="$1"
  local existing
  for existing in "${FAILED_ENTITIES[@]:-}"; do
    if [[ "${existing}" == "${entity}" ]]; then
      return
    fi
  done
  FAILED_ENTITIES+=("${entity}")
}

for entity in "${ENTITIES[@]}"; do
  entity_output_dir="${ENTITY_OUTPUT_ROOT}/${entity}"
  entity_log="${LOG_DIR}/${entity}.log"
  mkdir -p "${entity_output_dir}"
  echo "  - launch ${entity} (log: ${entity_log})"
  run_cli "${entity_log}" --entity "${entity}" --output-dir "${entity_output_dir}" "${COMMON_ARGS[@]}" &
  ENTITY_PIDS["${entity}"]=$!
done

remaining=${#ENTITIES[@]}
while [[ ${remaining} -gt 0 ]]; do
  monitor_latest_line_global
  progress=0

  for entity in "${ENTITIES[@]}"; do
    if [[ "${ENTITY_DONE[${entity}]:-0}" -eq 1 ]]; then
      continue
    fi

    pid="${ENTITY_PIDS[${entity}]}"
    if kill -0 "${pid}" 2>/dev/null; then
      continue
    fi

    progress=1
    if wait "${pid}"; then
      ENTITY_RCS["${entity}"]=0
      echo "  [ok] ${entity}"
    else
      rc=$?
      ENTITY_RCS["${entity}"]=${rc}
      add_failure "${entity}"
      echo "  [failed:${rc}] ${entity} (see ${LOG_DIR}/${entity}.log)"

      if [[ ${FAIL_FAST} -eq 1 ]]; then
        echo "  fail-fast: terminating remaining entity processes..."
        for other in "${ENTITIES[@]}"; do
          if [[ "${ENTITY_DONE[${other}]:-0}" -eq 0 && "${other}" != "${entity}" ]]; then
            kill "${ENTITY_PIDS[${other}]}" 2>/dev/null || true
          fi
        done
      fi
    fi

    ENTITY_DONE["${entity}"]=1
    remaining=$((remaining - 1))
  done

  if [[ ${remaining} -gt 0 && ${progress} -eq 0 ]]; then
    sleep "${LIVE_REFRESH_SECONDS}"
  fi
done

# Flush trailing live updates that may have landed right before process exit.
monitor_latest_line_global

parse_end=$(date +%s)
echo "[2/3] Done in $((parse_end - parse_start))s"

if [[ ${#FAILED_ENTITIES[@]} -gt 0 ]]; then
  echo "Entity parsing failed for: ${FAILED_ENTITIES[*]}" >&2
  for entity in "${FAILED_ENTITIES[@]}"; do
    echo "---- tail ${entity}.log ----" >&2
    tail -n 20 "${LOG_DIR}/${entity}.log" >&2 || true
  done
  exit 1
fi

echo "[3/3] Merging per-entity CSV outputs..."
merge_start=$(date +%s)

find "${OUTPUT_DIR}" -maxdepth 1 -type f -name "*.csv" -delete

declare -a MERGE_ORDER
MERGE_ORDER=("${ENTITIES[@]}")

copied_count=0
same_count=0
reference_conflict_count=0
MERGE_CONFLICTS=()
merge_processed=0

for entity in "${MERGE_ORDER[@]}"; do
  entity_output_dir="${ENTITY_OUTPUT_ROOT}/${entity}"
  shopt -s nullglob
  files=("${entity_output_dir}"/*.csv)
  shopt -u nullglob

  for src_file in "${files[@]}"; do
    merge_processed=$((merge_processed + 1))
    if (( merge_processed % 50 == 0 )); then
      echo "  [merge] processed ${merge_processed} files..."
    fi

    file_name="$(basename "${src_file}")"
    dst_file="${OUTPUT_DIR}/${file_name}"

    if [[ ! -f "${dst_file}" ]]; then
      cp "${src_file}" "${dst_file}"
      copied_count=$((copied_count + 1))
      continue
    fi

    if cmp -s "${src_file}" "${dst_file}"; then
      same_count=$((same_count + 1))
      continue
    fi

    if is_reference_table_file "${file_name}"; then
      reference_conflict_count=$((reference_conflict_count + 1))
      echo "  [warn] keeping existing reference table ${file_name}; conflicting copy from ${entity} ignored"
      continue
    fi

    MERGE_CONFLICTS+=("${file_name} (entity: ${entity})")
  done
done

if [[ ${#MERGE_CONFLICTS[@]} -gt 0 ]]; then
  echo "Merge conflicts detected in non-reference tables:" >&2
  for conflict in "${MERGE_CONFLICTS[@]}"; do
    echo "  - ${conflict}" >&2
  done
  exit 1
fi

merge_end=$(date +%s)
end_time=$(date +%s)

echo "[3/3] Done in $((merge_end - merge_start))s"
echo "Merge summary: copied=${copied_count}, identical_skips=${same_count}, reference_conflicts=${reference_conflict_count}"
echo "All done in $((end_time - start_time))s"
echo "Final CSV output: ${OUTPUT_DIR}"
echo "Reference IDs: ${REFERENCE_DIR}"
if [[ ${KEEP_TEMP} -eq 1 ]]; then
  echo "Temporary outputs kept at: ${TMP_ROOT}"
fi
