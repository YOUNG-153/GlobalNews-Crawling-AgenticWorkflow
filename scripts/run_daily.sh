#!/usr/bin/env bash
# =============================================================================
# GlobalNews Crawling & Analysis System -- Daily Pipeline Runner
#
# Triggered by cron at 02:00 AM daily. Executes the full crawl + analysis
# pipeline with comprehensive error handling, lock file management, health
# checks, log rotation, and timeout protection.
#
# Usage:
#   scripts/run_daily.sh              # Normal execution
#   scripts/run_daily.sh --dry-run    # Validate without execution
#   scripts/run_daily.sh --date 2026-02-25  # Specific date
#
# CRON SETUP (add via: crontab -e):
#   0 2 * * * cd /path/to/project && .venv/bin/python scripts/run_daily.sh >> data/logs/cron.log 2>&1
#   Ensure .venv/bin/python points to Python 3.12-3.13 (NOT system 3.14+)
#
# Exit codes:
#   0 -- Pipeline completed successfully
#   1 -- Pipeline failed (see error logs)
#   2 -- Pre-run health check failed
#   3 -- Lock acquisition failed (another instance running)
#   4 -- Timeout (pipeline exceeded 4-hour limit)
#
# Reference:
#   PRD Section 12.2 -- auto-execution success rate >= 95%
#   Step 5 Architecture Blueprint, Section 8 (Operational Requirements)
# =============================================================================

set -euo pipefail

# =============================================================================
# Configuration
# =============================================================================

# Auto-detect project root (directory containing main.py)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Date handling
TARGET_DATE="${2:-$(date +%Y-%m-%d)}"
DRY_RUN=false

# Timeout: 4 hours (14400 seconds)
PIPELINE_TIMEOUT=14400

# Log paths
LOG_DIR="${PROJECT_DIR}/data/logs"
DAILY_LOG="${LOG_DIR}/daily/$(date +%Y-%m-%d)-daily.log"
ERROR_LOG="${LOG_DIR}/errors.log"
ALERT_DIR="${LOG_DIR}/alerts"

# Lock name
LOCK_NAME="daily"

# Python interpreter — overridden by activate_venv() when .venv is found
PYTHON="${PYTHON:-python3}"

# =============================================================================
# Argument Parsing
# =============================================================================

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --date)
            TARGET_DATE="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1" >&2
            echo "Usage: $0 [--dry-run] [--date YYYY-MM-DD]" >&2
            exit 1
            ;;
    esac
done

# =============================================================================
# Utility Functions
# =============================================================================

timestamp() {
    date -u +"%Y-%m-%dT%H:%M:%SZ"
}

log_info() {
    local msg="[$(timestamp)] [INFO]  $1"
    echo "$msg" | tee -a "${DAILY_LOG}"
}

log_error() {
    local msg="[$(timestamp)] [ERROR] $1"
    echo "$msg" | tee -a "${DAILY_LOG}" >&2
    echo "$msg" >> "${ERROR_LOG}"
}

log_warn() {
    local msg="[$(timestamp)] [WARN]  $1"
    echo "$msg" | tee -a "${DAILY_LOG}"
}

write_alert() {
    local alert_file="${ALERT_DIR}/$(date +%Y-%m-%d)-daily-failure.log"
    {
        echo "=========================================="
        echo "DAILY PIPELINE FAILURE ALERT"
        echo "=========================================="
        echo "Date:    ${TARGET_DATE}"
        echo "Time:    $(timestamp)"
        echo "Host:    $(hostname)"
        echo "PID:     $$"
        echo ""
        echo "Error:"
        echo "  $1"
        echo ""
        echo "Last 50 lines of daily log:"
        if [[ -f "${DAILY_LOG}" ]]; then
            tail -50 "${DAILY_LOG}"
        else
            echo "  (no daily log available)"
        fi
        echo ""
        echo "=========================================="
    } >> "${alert_file}"
    log_error "Alert written to: ${alert_file}"
}

# =============================================================================
# Virtual Environment Detection and Activation
# =============================================================================

activate_venv() {
    # Try common virtualenv locations
    local venv_candidates=(
        "${PROJECT_DIR}/.venv"
        "${PROJECT_DIR}/venv"
        "${PROJECT_DIR}/env"
    )

    for venv_path in "${venv_candidates[@]}"; do
        if [[ -f "${venv_path}/bin/activate" ]]; then
            log_info "Activating virtualenv: ${venv_path}"
            # shellcheck disable=SC1091
            source "${venv_path}/bin/activate"
            PYTHON="${venv_path}/bin/python"
            return 0
        fi
    done

    # No venv found -- check if system Python has required packages
    if "${PYTHON}" -c "import yaml; import requests" 2>/dev/null; then
        log_warn "No virtualenv found. Using system Python: $(which "${PYTHON}")"
        return 0
    fi

    log_error "No virtualenv found and system Python missing critical deps."
    log_error "Searched: ${venv_candidates[*]}"
    log_error "Install: python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
    return 1
}

# =============================================================================
# Lock Management (via Python self_recovery module)
# =============================================================================

acquire_lock() {
    log_info "Acquiring lock: ${LOCK_NAME}"
    local result
    result=$("${PYTHON}" -m src.utils.self_recovery \
        --project-dir "${PROJECT_DIR}" \
        --acquire-lock "${LOCK_NAME}" 2>/dev/null) || true

    if echo "${result}" | "${PYTHON}" -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if d.get('acquired') else 1)" 2>/dev/null; then
        log_info "Lock acquired successfully"
        return 0
    else
        log_error "Failed to acquire lock. Another instance may be running."
        return 1
    fi
}

release_lock() {
    log_info "Releasing lock: ${LOCK_NAME}"
    "${PYTHON}" -m src.utils.self_recovery \
        --project-dir "${PROJECT_DIR}" \
        --force-release-lock "${LOCK_NAME}" 2>/dev/null || true
}

# =============================================================================
# Health Check
# =============================================================================

run_health_check() {
    log_info "Running pre-run health checks..."
    local result
    result=$("${PYTHON}" -m src.utils.self_recovery \
        --project-dir "${PROJECT_DIR}" \
        --health-check 2>/dev/null)
    local rc=$?

    if [[ ${rc} -ne 0 ]]; then
        log_error "Health check failed:"
        echo "${result}" | tee -a "${DAILY_LOG}" >&2
        return 1
    fi

    # Log disk space
    local disk_free
    disk_free=$(echo "${result}" | "${PYTHON}" -c "import sys,json; print(json.load(sys.stdin).get('disk_free_gb', '?'))" 2>/dev/null || echo "?")
    log_info "Health check passed. Disk free: ${disk_free} GB"
    return 0
}

# =============================================================================
# Log Rotation
# =============================================================================

rotate_logs() {
    log_info "Running log rotation..."
    local daily_log_dir="${LOG_DIR}/daily"

    if [[ ! -d "${daily_log_dir}" ]]; then
        return 0
    fi

    # Calculate directory size in bytes
    local dir_size
    dir_size=$(du -sk "${daily_log_dir}" 2>/dev/null | cut -f1 || echo "0")
    local dir_size_mb=$((dir_size / 1024))

    log_info "Daily log directory size: ${dir_size_mb} MB"

    # Rotate if > 500MB
    if [[ ${dir_size_mb} -gt 500 ]]; then
        log_info "Log directory exceeds 500MB. Removing logs older than 30 days..."
        find "${daily_log_dir}" -type f -name "*.log" -mtime +30 -delete 2>/dev/null || true
        local new_size
        new_size=$(du -sk "${daily_log_dir}" 2>/dev/null | cut -f1 || echo "0")
        log_info "Log directory after rotation: $((new_size / 1024)) MB"
    fi

    # Also run the Python cleanup for data logs
    "${PYTHON}" -m src.utils.self_recovery \
        --project-dir "${PROJECT_DIR}" \
        --cleanup 2>/dev/null || true
}

# =============================================================================
# Cleanup
# =============================================================================

cleanup_on_exit() {
    local exit_code=$?
    release_lock
    if [[ ${exit_code} -ne 0 ]]; then
        log_error "Pipeline exited with code ${exit_code}"
    fi
}

# =============================================================================
# Main Pipeline Execution
# =============================================================================

run_pipeline() {
    local mode_args=("--mode" "full" "--date" "${TARGET_DATE}")

    if [[ "${DRY_RUN}" == "true" ]]; then
        mode_args+=("--dry-run")
        log_info "DRY RUN mode enabled"
    fi

    log_info "Starting pipeline: ${PYTHON} main.py ${mode_args[*]}"
    log_info "Target date: ${TARGET_DATE}"
    log_info "Project dir: ${PROJECT_DIR}"
    log_info "PID: $$"

    local start_time
    start_time=$(date +%s)

    # Execute with timeout
    local pipeline_exit_code=0
    if command -v timeout >/dev/null 2>&1; then
        # GNU timeout (Linux)
        timeout "${PIPELINE_TIMEOUT}" "${PYTHON}" "${PROJECT_DIR}/main.py" "${mode_args[@]}" \
            >> "${DAILY_LOG}" 2>&1 || pipeline_exit_code=$?
    elif command -v gtimeout >/dev/null 2>&1; then
        # GNU timeout via coreutils (macOS with brew install coreutils)
        gtimeout "${PIPELINE_TIMEOUT}" "${PYTHON}" "${PROJECT_DIR}/main.py" "${mode_args[@]}" \
            >> "${DAILY_LOG}" 2>&1 || pipeline_exit_code=$?
    else
        # Fallback: background process with manual timeout check
        "${PYTHON}" "${PROJECT_DIR}/main.py" "${mode_args[@]}" \
            >> "${DAILY_LOG}" 2>&1 &
        local bg_pid=$!

        # Wait for completion or timeout
        local elapsed=0
        while kill -0 "${bg_pid}" 2>/dev/null; do
            sleep 10
            elapsed=$((elapsed + 10))
            if [[ ${elapsed} -ge ${PIPELINE_TIMEOUT} ]]; then
                log_error "Pipeline timed out after ${PIPELINE_TIMEOUT}s. Killing PID ${bg_pid}"
                kill -TERM "${bg_pid}" 2>/dev/null || true
                sleep 5
                kill -KILL "${bg_pid}" 2>/dev/null || true
                pipeline_exit_code=124  # Matches GNU timeout exit code
                break
            fi
        done

        if [[ ${pipeline_exit_code} -eq 0 ]]; then
            wait "${bg_pid}" 2>/dev/null || pipeline_exit_code=$?
        fi
    fi

    local end_time
    end_time=$(date +%s)
    local elapsed=$((end_time - start_time))
    local elapsed_min=$((elapsed / 60))

    if [[ ${pipeline_exit_code} -eq 124 ]]; then
        log_error "Pipeline TIMED OUT after ${elapsed_min} minutes (limit: $((PIPELINE_TIMEOUT / 60)) min)"
        write_alert "Pipeline timed out after ${elapsed_min} minutes"
        return 4
    elif [[ ${pipeline_exit_code} -ne 0 ]]; then
        log_error "Pipeline FAILED with exit code ${pipeline_exit_code} after ${elapsed_min} minutes"
        write_alert "Pipeline failed with exit code ${pipeline_exit_code}"
        return 1
    fi

    log_info "Pipeline completed successfully in ${elapsed_min} minutes"
    return 0
}

# =============================================================================
# Entry Point
# =============================================================================

main() {
    # Ensure log directories exist
    mkdir -p "${LOG_DIR}/daily" "${LOG_DIR}/alerts" "${LOG_DIR}/cron"

    log_info "============================================"
    log_info "GlobalNews Daily Pipeline -- START"
    log_info "============================================"
    log_info "Date: $(date -u)"
    log_info "Target: ${TARGET_DATE}"
    log_info "Host: $(hostname)"
    log_info "Dry run: ${DRY_RUN}"

    # Set trap for cleanup on exit
    trap cleanup_on_exit EXIT

    # Change to project directory
    cd "${PROJECT_DIR}"

    # Step 1: Activate virtual environment
    if ! activate_venv; then
        write_alert "Virtual environment activation failed"
        exit 2
    fi

    # Step 2: Health check
    if ! run_health_check; then
        write_alert "Pre-run health check failed"
        exit 2
    fi

    # Step 3: Acquire lock
    if ! acquire_lock; then
        write_alert "Lock acquisition failed -- another instance running"
        # Override the trap since we do not hold the lock
        trap - EXIT
        exit 3
    fi

    # Step 4: Run the pipeline
    local pipeline_result=0
    run_pipeline || pipeline_result=$?

    # Step 5: Post-run log rotation
    rotate_logs

    # Step 6: Generate daily summary
    log_info "============================================"
    if [[ ${pipeline_result} -eq 0 ]]; then
        log_info "GlobalNews Daily Pipeline -- SUCCESS"
    else
        log_info "GlobalNews Daily Pipeline -- FAILED (exit: ${pipeline_result})"
    fi
    log_info "============================================"

    exit "${pipeline_result}"
}

main "$@"
