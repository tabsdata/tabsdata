#!/usr/bin/env bash
#
# Copyright 2026 Tabsdata Inc.
#

set -euo pipefail

STARTED="${EPOCHREALTIME:-$(date +%s)}"

die() {
   echo "$1 failed with exit code ${2:-unknown}" >&2
   exit 1
}

now() {
   echo "${EPOCHREALTIME:-$(date +%s)}"
}

elapsed_since() {
   awk -v from="$1" -v to="$2" 'BEGIN { printf "%.2f", to - from }'
}

trace() {
   printf '>> [%8ss] %s\n' "$(elapsed_since "$STARTED" "$(now)")" "$1"
}

step() {
   local description="$1"
   shift
   local started
   started="$(now)"
   trace "BEGIN $description"
   local status=0
   "$@" || status="$?"
   if (( status != 0 )); then
      trace "FAILED $description after $(elapsed_since "$started" "$(now)")s"
      die "$description" "$status"
   fi
   trace "END   $description in $(elapsed_since "$started" "$(now)")s"
}

run() {
   local started
   started="$(now)"
   trace "RUN $*"
   local status=0
   "$@" || status="$?"
   if (( status != 0 )); then
      trace "RAN in $(elapsed_since "$started" "$(now)")s with exit code $status: $(basename "$1")"
      return "$status"
   fi
   trace "RAN in $(elapsed_since "$started" "$(now)")s: $(basename "$1")"
}

LOADER="$(cd "$(dirname "$0")" && pwd)/venvloader.py"

PYTHON_BINARY="python3"
UV_BINARY="uv"
TAR_BINARY="tar"
PIP_PACKAGES=("boto3" "pyyaml" "zstandard")

show_usage() {
   echo "Usage: $0 [options]"
   echo "  -o, --output FOLDER            Runtimes folder holding the uv home  (mandatory)"
   echo "      --storage-config FILE      Path to apiserver.storage.yaml       (mandatory)"
   echo "      --mount ID                 Storage mount id                     (default: IMAGES)"
   echo "      --subfolder FOLDER         Subfolder under the mount            (default: python)"
   echo "      --image NAME               Image file name; the venv name is appended to the stem    (default: tdvenvs.tar.zst)"
   echo "      --venv NAME                Virtual environment to load          (default: td)"
   echo "  -h, --help                     Show this help"
}

parse_arguments() {
   output=""
   storage_config=""
   mount=""
   subfolder=""
   image=""
   venv=""
   while [[ $# -gt 0 ]]; do
      case "$1" in
         -o|--output)        output="$2";         shift 2 ;;
         --storage-config)   storage_config="$2"; shift 2 ;;
         --mount)            mount="$2";          shift 2 ;;
         --subfolder)        subfolder="$2";      shift 2 ;;
         --image)            image="$2";          shift 2 ;;
         --venv)             venv="$2";           shift 2 ;;
         -h|--help)          show_usage; exit 0 ;;
         *) echo "Unknown option: $1" >&2; show_usage; exit 2 ;;
      esac
   done
}

validate_arguments() {
   if [[ -z "$output" ]]; then
      echo "Output folder is mandatory" >&2
      show_usage
      exit 2
   fi
   if [[ -z "$storage_config" ]]; then
      echo "Storage configuration is mandatory" >&2
      show_usage
      exit 2
   fi
   if [[ ! -f "$storage_config" ]]; then
      echo "Storage configuration not found: $storage_config" >&2
      exit 1
   fi
   if [[ ! -f "$LOADER" ]]; then
      echo "The loader script is not present: $LOADER" >&2
      exit 1
   fi
}

install_prerequisites() {
   if command -v "$UV_BINARY" >/dev/null 2>&1; then
      run "$UV_BINARY" pip install --system --quiet "${PIP_PACKAGES[@]}"
   else
      run "$PYTHON_BINARY" -m pip install --quiet --disable-pip-version-check "${PIP_PACKAGES[@]}"
   fi
   if command -v "$TAR_BINARY" >/dev/null 2>&1; then
      trace "Unpacking with $TAR_BINARY"
   else
      trace "The $TAR_BINARY binary is not present; unpacking with python tarfile"
   fi
}

load_image() {
   local arguments=(--output "$output" --storage-config "$storage_config")
   if [[ -n "$mount" ]]; then
      arguments+=(--mount "$mount")
   fi
   if [[ -n "$subfolder" ]]; then
      arguments+=(--subfolder "$subfolder")
   fi
   if [[ -n "$image" ]]; then
      arguments+=(--image "$image")
   fi
   if [[ -n "$venv" ]]; then
      arguments+=(--venv "$venv")
   fi
   run "$PYTHON_BINARY" "$LOADER" "${arguments[@]}"
}

main() {
   parse_arguments "$@"
   validate_arguments

   step "Installing prerequisites" install_prerequisites
   step "Loading the image"        load_image

   local elapsed
   elapsed="$(elapsed_since "$STARTED" "$(now)")"
   local hours
   hours="$(awk "BEGIN { printf \"%.4f\", $elapsed / 3600 }")"
   echo ">> Done!"
   echo ">> Elapsed time: $hours hours"
}

main "$@"
