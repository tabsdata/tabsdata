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

PYTHON_VERSION="3.12.13"

PYTHON_MAJOR="3"
PYTHON_MINOR="12"
PYTHON_MINIMUM_PATCH="11"

IMAGE_NAME="tdvenvs"
IMAGE_SUFFIX=".tar.zst"

UV_FOLDER=".uv"
UV_PYTHON_FOLDER="python"
UV_KENVS_FOLDER="kenvs"

TOOLS_HOME="$HOME/.tools"
TOOLS_BIN_FOLDER="bin"
TOOLS_KENVS_FOLDER="kenvs"
TOOLS_BIN_HOME="$TOOLS_HOME/$TOOLS_BIN_FOLDER"
TOOLS_KENVS_HOME="$TOOLS_HOME/$TOOLS_KENVS_FOLDER"

VENV_TD="td"
VENV_Z="z"

UV_INSTALLER="https://astral.sh/uv/install.sh"

ZSTANDARD_PACKAGE="zstandard"

ARCHIVE_MODULE="tabsdatak.common._interpreter.python.archive"

ARCHIVE_COMPRESSION_LEVEL="3"
ARCHIVE_COMPRESSION_THREADS="-1"
ARCHIVE_LONG_DISTANCE_WINDOW_LOG="27"

show_usage() {
   echo "Usage: $0 [options]"
   echo "  -p, --python VERSION           Python version                   (default: $PYTHON_VERSION)"
   echo "      --requirements-td FILE     Complementary packages for td    (default: none)"
   echo "  -o, --output FOLDER            Output folder                    (mandatory)"
   echo "      --storage-config FILE      Storage configuration used to copy the image     (default: none)"
   echo "  -h, --help                     Show this help"
}

parse_arguments() {
   python_version="$PYTHON_VERSION"
   requirements_td=""
   output=""
   storage_config=""
   transporter_request=""
   transporter_response=""
   while [[ $# -gt 0 ]]; do
      case "$1" in
         -p|--python)         python_version="$2";  shift 2 ;;
         --requirements-td)   requirements_td="$2"; shift 2 ;;
         -o|--output)         output="$2";          shift 2 ;;
         --storage-config)    storage_config="$2";  shift 2 ;;
         --transporter-request)  transporter_request="$2";  shift 2 ;;
         --transporter-response) transporter_response="$2"; shift 2 ;;
         -h|--help)           show_usage; exit 0 ;;
         *) echo "Unknown option: $1" >&2; show_usage; exit 2 ;;
      esac
   done
}

validate_arguments() {
   if [[ -z "$python_version" ]]; then
      echo "Python version must not be empty" >&2
      exit 2
   fi
   if [[ ! "$python_version" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
      echo "Python version must have the form major.minor.patch, for example 3.12.13" >&2
      exit 2
   fi
   major="${BASH_REMATCH[1]}"
   minor="${BASH_REMATCH[2]}"
   patch="${BASH_REMATCH[3]}"
   if [[ "$major" -ne "$PYTHON_MAJOR" || "$minor" -ne "$PYTHON_MINOR" || "$patch" -lt "$PYTHON_MINIMUM_PATCH" ]]; then
      echo "Python version must be $PYTHON_MAJOR.$PYTHON_MINOR.$PYTHON_MINIMUM_PATCH or higher within the $PYTHON_MAJOR.$PYTHON_MINOR series" >&2
      exit 2
   fi

   if [[ -n "$requirements_td" && ! -f "$requirements_td" ]]; then
      echo "Requirements file for td not found: $requirements_td" >&2
      exit 1
   fi

   if [[ -z "$output" ]]; then
      echo "Output folder is mandatory" >&2
      show_usage
      exit 2
   fi

   if [[ -n "$storage_config" && ! -f "$storage_config" ]]; then
      echo "Storage configuration file not found: $storage_config" >&2
      exit 1
   fi

   if [[ -n "$storage_config" && -z "$transporter_request" ]]; then
      echo "Transporter request file is mandatory when a storage configuration is given" >&2
      exit 2
   fi

   if [[ -n "$storage_config" && -z "$transporter_response" ]]; then
      echo "Transporter response file is mandatory when a storage configuration is given" >&2
      exit 2
   fi
}

setup_workspace() {
   work="$(mktemp -d "${TMPDIR:-/tmp}/tdvenvs.XXXXXX")"
   output_image="$output/$IMAGE_NAME$IMAGE_SUFFIX"
   UV_HOME="$output/$UV_FOLDER"
   UV_PYTHON_HOME="$UV_HOME/$UV_PYTHON_FOLDER"
   UV_KENVS_HOME="$UV_HOME/$UV_KENVS_FOLDER"
   virtual_environment_td="$UV_KENVS_HOME/$VENV_TD"
   virtual_environment_z="$TOOLS_KENVS_HOME/$VENV_Z"
   export UV_PYTHON_INSTALL_DIR="$UV_PYTHON_HOME"
   echo ">> Using Python $python_version in work folder $work"
   echo ">> Using uv home $UV_HOME"
   echo ">> Using tools home $TOOLS_HOME"
}

install_prerequisites() {
   export DEBIAN_FRONTEND=noninteractive
   run apt-get -o Acquire::Retries=5 -o Acquire::http::Timeout=30 update
   run apt-get -o Acquire::Retries=5 -o Acquire::http::Timeout=30 install --yes curl ca-certificates
}

install_uv() {
   mkdir -p "$TOOLS_BIN_HOME"
   curl -LsSf "$UV_INSTALLER" \
      | env UV_INSTALL_DIR="$TOOLS_BIN_HOME" UV_UNMANAGED_INSTALL="$TOOLS_BIN_HOME" sh
   export PATH="$TOOLS_BIN_HOME:$PATH"
   run uv --version
}

install_python() {
   run uv python install "$python_version"
   run uv python find "$python_version"
}

remove_virtual_environment() {
   if [[ -d "$1" ]]; then
      echo ">> Removing the existing virtual environment $1"
      rm -rf "$1"
   fi
}

create_compressor_environment() {
   remove_virtual_environment "$virtual_environment_z"
   run uv venv --python-preference only-managed --python "$python_version" "$virtual_environment_z"
   run uv pip install --python "$virtual_environment_z/bin/python" "$ZSTANDARD_PACKAGE"
}

create_virtual_environment() {
   remove_virtual_environment "$virtual_environment_td"
   run uv venv --seed --relocatable --python-preference only-managed --python "$python_version" "$virtual_environment_td"
   run uv pip install --python "$virtual_environment_td/bin/python" --upgrade pip
}

install_packages() {
   if [[ -n "$requirements_td" ]]; then
      run uv pip install --python "$virtual_environment_td/bin/python" "tabsdata[all]" -r "$requirements_td"
   else
      run uv pip install --python "$virtual_environment_td/bin/python" "tabsdata[all]"
   fi
}

verify_image() {
   test -d "$UV_PYTHON_HOME"
   test -d "$virtual_environment_td"
}

compress_image() {
   cat > "$work/compress.py" <<'PYTHON'
import sys
import zstandard

source, target = sys.argv[1], sys.argv[2]
level, threads, window_log = int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
parameters = zstandard.ZstdCompressionParameters.from_level(
    level,
    threads=threads,
    window_log=window_log,
    write_content_size=True,
)
compressor = zstandard.ZstdCompressor(compression_params=parameters)
with open(source, "rb") as reader, open(target, "wb") as writer:
    compressor.copy_stream(reader, writer)
PYTHON
   "$virtual_environment_z/bin/python" "$work/compress.py" \
      "$1" "$2" \
      "$ARCHIVE_COMPRESSION_LEVEL" \
      "$ARCHIVE_COMPRESSION_THREADS" \
      "$ARCHIVE_LONG_DISTANCE_WINDOW_LOG"
}

write_image() {
   mkdir -p "$output"
   run tar -cf "$work/$IMAGE_NAME.tar" -C "$UV_HOME" "$UV_PYTHON_FOLDER" "$UV_KENVS_FOLDER"
   compress_image "$work/$IMAGE_NAME.tar" "$output_image"
   rm -rf "$work"
}

send_image() {
   if [[ -z "$storage_config" ]]; then
      echo ">> Skipping the image upload; no storage configuration given"
      return
   fi
   run "$virtual_environment_td/bin/python" -m "$ARCHIVE_MODULE" \
      --image "$output_image" \
      --storage-config "$storage_config" \
      --transporter-request "$transporter_request" \
      --transporter-response "$transporter_response"
}

main() {
   local start="$SECONDS"

   parse_arguments "$@"
   validate_arguments
   setup_workspace
   step "Installing prerequisites"           install_prerequisites
   step "Installing uv"                      install_uv
   step "Installing Python"                  install_python
   step "Creating the compressor environment" create_compressor_environment
   step "Creating the virtual environments"   create_virtual_environment
   step "Installing packages"                install_packages
   step "Verifying the image"                 verify_image
   step "Writing the image"                   write_image
   step "Sending the image"                   send_image

   local elapsed="$(( SECONDS - start ))"
   local hours="$(awk "BEGIN { printf \"%.4f\", $elapsed / 3600 }")"
   echo ">> Done! Image written to $output_image"
   echo ">> Elapsed time: $hours hours"
}

main "$@"
