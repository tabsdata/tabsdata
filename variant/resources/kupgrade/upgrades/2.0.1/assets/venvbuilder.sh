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
PYTHON_MINIMUM_PATCH="13"

IMAGE_NAME="tdvenvs"
IMAGE_SUFFIX=".tar.zst"

UV_FOLDER=".uv"
UV_PYTHON_FOLDER="python"
UV_KENVS_FOLDER="kenvs"
UV_LINK_MODE_DEFAULT="${UV_LINK_MODE:-copy}"

TOOLS_HOME="$HOME/.tools"
TOOLS_BIN_FOLDER="bin"
TOOLS_KENVS_FOLDER="kenvs"
TOOLS_BIN_HOME="$TOOLS_HOME/$TOOLS_BIN_FOLDER"
TOOLS_KENVS_HOME="$TOOLS_HOME/$TOOLS_KENVS_FOLDER"

VENV_TD="td"
VENV_FN="fn"
VENV_Z="z"

UV_INSTALLER="https://astral.sh/uv/install.sh"

ZSTANDARD_PACKAGE="zstandard"

ARCHIVE_MODULE="tabsdatak.common._interpreter.python.archive"

ARCHIVE_COMPRESSION_LEVEL="3"
ARCHIVE_COMPRESSION_THREADS="-1"
ARCHIVE_LONG_DISTANCE_WINDOW_LOG="27"

LIBEXEC_FOLDER="libexec/tabsdata"
BIN_FOLDER="bin"

PRUNED_LIBEXEC_FILES=(
   "control-plane"
   "etcd"
   "kube-apiserver"
   "kube-controller-manager"
   "pod-runner"
   "virtual-kubelet"
)

PRUNED_BIN_FILES=(
   "km"
   "kmc"
   "td_about"
   "tdkserver"
)

PRUNED_BIN_FILES_FN=(
   "${PRUNED_BIN_FILES[@]}"
   "td_apiserver"
   "td_dbmanager"
)

show_usage() {
   echo "Usage: $0 [options]"
   echo "  -p, --python VERSION           Python version                   (default: $PYTHON_VERSION)"
   echo "      --requirements-td FILE     Complementary packages for td    (default: none)"
   echo "      --requirements-fn FILE     Complementary packages for fn    (default: none)"
   echo "      --constraint FILE          Constraint file pinning the versions of every install    (default: none)"
   echo "  -o, --output FOLDER            Output folder                    (mandatory)"
   echo "      --version VERSION          Tabsdata version to install      (mandatory)"
   echo "      --image NAME               Image file name; the venv name is appended to the stem, yielding one image per virtual environment    (default: $IMAGE_NAME$IMAGE_SUFFIX)"
   echo "      --storage-config FILE      Storage configuration used to copy the image     (default: none)"
   echo "  -h, --help                     Show this help"
}

constrained() {
   if [[ -n "$constraint" ]]; then
      echo "--constraint $constraint"
   fi
}

parse_arguments() {
   python_version="$PYTHON_VERSION"
   requirements_td=""
   requirements_fn=""
   constraint=""
   output=""
   image="$IMAGE_NAME$IMAGE_SUFFIX"
   version=""
   storage_config=""
   transporter_request=""
   transporter_response=""
   while [[ $# -gt 0 ]]; do
      case "$1" in
         -p|--python)         python_version="$2";  shift 2 ;;
         --requirements-td)   requirements_td="$2"; shift 2 ;;
         --requirements-fn)   requirements_fn="$2"; shift 2 ;;
         --constraint)        constraint="$2";      shift 2 ;;
         -o|--output)         output="$2";          shift 2 ;;
         --image)             image="$2";           shift 2 ;;
         --version)           version="$2";         shift 2 ;;
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

   if [[ -n "$constraint" && ! -f "$constraint" ]]; then
      echo "Constraint file not found: $constraint" >&2
      exit 1
   fi
   if [[ -n "$requirements_td" && ! -f "$requirements_td" ]]; then
      echo "Requirements file for td not found: $requirements_td" >&2
      exit 1
   fi
   if [[ -n "$requirements_fn" && ! -f "$requirements_fn" ]]; then
      echo "Requirements file for fn not found: $requirements_fn" >&2
      exit 1
   fi

   if [[ -z "$output" ]]; then
      echo "Output folder is mandatory" >&2
      show_usage
      exit 2
   fi

   if [[ -z "$image" ]]; then
      echo "Image file name must not be empty" >&2
      exit 2
   fi

   if [[ -z "$version" ]]; then
      echo "Tabsdata version is mandatory" >&2
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

image_for_venv() {
   local venv="$1"
   local base="$image"
   if [[ "$base" == *"$IMAGE_SUFFIX" ]]; then
      base="${base%$IMAGE_SUFFIX}"
   fi
   echo "$base-$venv$IMAGE_SUFFIX"
}

human_size() {
   local bytes="$1"
   awk -v b="$bytes" 'BEGIN {
      split("B KiB MiB GiB TiB", unit, " ")
      i = 1
      while (b >= 1024 && i < 5) { b /= 1024; i++ }
      printf (i == 1 ? "%d %s" : "%.2f %s"), b, unit[i]
   }'
}

file_size() {
   wc -c < "$1" | tr -d ' '
}

setup_workspace() {
   work="$(mktemp -d "${TMPDIR:-/tmp}/tdvenvs.XXXXXX")"
   output_image_td="$output/$(image_for_venv "$VENV_TD")"
   output_image_fn="$output/$(image_for_venv "$VENV_FN")"
   UV_HOME="$output/$UV_FOLDER"
   UV_PYTHON_HOME="$UV_HOME/$UV_PYTHON_FOLDER"
   UV_KENVS_HOME="$UV_HOME/$UV_KENVS_FOLDER"
   virtual_environment_td="$UV_KENVS_HOME/$VENV_TD"
   virtual_environment_fn="$UV_KENVS_HOME/$VENV_FN"
   virtual_environment_z="$TOOLS_KENVS_HOME/$VENV_Z"
   export UV_PYTHON_INSTALL_DIR="$UV_PYTHON_HOME"
   export UV_LINK_MODE="$UV_LINK_MODE_DEFAULT"
   echo ">> Using Python $python_version in work folder $work"
   echo ">> Using uv home $UV_HOME"
   echo ">> Using tools home $TOOLS_HOME"
   echo ">> Using uv link mode $UV_LINK_MODE"
}

report_indexes() {
   local variable
   for variable in UV_DEFAULT_INDEX UV_INDEX UV_INDEX_STRATEGY; do
      printf ">> %-18s = %s\n" "$variable" "${!variable:-<unset>}"
   done
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
   run uv pip install --python "$virtual_environment_z/bin/python" $(constrained) "$ZSTANDARD_PACKAGE"
}

create_virtual_environment() {
   remove_virtual_environment "$virtual_environment_td"
   run uv venv --seed --relocatable --python-preference only-managed --python "$python_version" "$virtual_environment_td"
   run uv pip install --python "$virtual_environment_td/bin/python" $(constrained) --upgrade pip
   remove_virtual_environment "$virtual_environment_fn"
   run uv venv --seed --relocatable --python-preference only-managed --python "$python_version" "$virtual_environment_fn"
   run uv pip install --python "$virtual_environment_fn/bin/python" $(constrained) --upgrade pip
}

has_requirements() {
   local file="$1"
   [[ -n "$file" && -f "$file" ]] || return 1
   grep -qE '^[[:space:]]*[^[:space:]#]' "$file"
}

install_packages() {
   if has_requirements "$requirements_td"; then
      run uv pip install --python "$virtual_environment_td/bin/python" $(constrained) "tabsdata[all]==$version" -r "$requirements_td"
   else
      run uv pip install --python "$virtual_environment_td/bin/python" $(constrained) "tabsdata[all]==$version"
   fi
   if has_requirements "$requirements_fn"; then
      run uv pip install --python "$virtual_environment_fn/bin/python" $(constrained) "tabsdata[all]==$version" -r "$requirements_fn"
   else
      run uv pip install --python "$virtual_environment_fn/bin/python" $(constrained) "tabsdata[all]==$version"
   fi
}

prune_one_file() {
   local folder="$1"
   local name="$2"
   local target="$folder/$name"
   if [[ ! -e "$target" && ! -L "$target" ]]; then
      trace "Skipping the prune of $target; it does not exist"
      return 1
   fi
   rm -f "$target"
   trace "Pruned $target"
}

prune_one_group() {
   local folder="$1"
   shift
   local total="$#"
   local pruned=0
   local name
   for name in "$@"; do
      if prune_one_file "$folder" "$name"; then
         pruned=$(( pruned + 1 ))
      fi
   done
   trace "Pruned $pruned of $total file(s) from $folder"
}

prune_one_virtual_environment() {
   local virtual_environment="$1"
   shift
   prune_one_group "$virtual_environment/$LIBEXEC_FOLDER" "${PRUNED_LIBEXEC_FILES[@]}"
   prune_one_group "$virtual_environment/$BIN_FOLDER" "$@"
}

prune_virtual_environment() {
   prune_one_virtual_environment "$virtual_environment_td" "${PRUNED_BIN_FILES[@]}"
   prune_one_virtual_environment "$virtual_environment_fn" "${PRUNED_BIN_FILES_FN[@]}"
}

verify_image() {
   test -d "$UV_PYTHON_HOME"
   test -d "$virtual_environment_td"
   test -d "$virtual_environment_fn"
}

write_compressor_script() {
   cat > "$work/compress.py" <<'PYTHON'
import sys
import zstandard

target = sys.argv[1]
level, threads, window_log = int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
parameters = zstandard.ZstdCompressionParameters.from_level(
    level,
    threads=threads,
    window_log=window_log,
    write_content_size=True,
)
compressor = zstandard.ZstdCompressor(compression_params=parameters)
with open(target, "wb") as writer:
    compressor.copy_stream(sys.stdin.buffer, writer)
PYTHON
}

write_one_image() {
   local venv="$1"
   local target="$2"
   local started
   started="$(now)"
   trace "Writing $target from $UV_PYTHON_FOLDER and $UV_KENVS_FOLDER/$venv"
   tar -cf - -C "$UV_HOME" "$UV_PYTHON_FOLDER" "$UV_KENVS_FOLDER/$venv" \
      | "$virtual_environment_z/bin/python" "$work/compress.py" \
         "$target" \
         "$ARCHIVE_COMPRESSION_LEVEL" \
         "$ARCHIVE_COMPRESSION_THREADS" \
         "$ARCHIVE_LONG_DISTANCE_WINDOW_LOG"
   local size
   size="$(file_size "$target")"
   trace "Wrote $target: $(human_size "$size") ($size bytes) in $(elapsed_since "$started" "$(now)")s"
}

write_image() {
   mkdir -p "$output"
   write_compressor_script
   write_one_image "$VENV_TD" "$output_image_td"
   write_one_image "$VENV_FN" "$output_image_fn"
   rm -rf "$work"
}

shuttle_path() {
   local base="$1"
   local tag="$2"
   local folder="$(dirname "$base")"
   local name="$(basename "$base")"
   echo "$folder/${name%.*}-$tag.${name##*.}"
}

send_one_image() {
   local target="$1"
   local tag="$2"
   local request="$(shuttle_path "$transporter_request" "$tag")"
   local response="$(shuttle_path "$transporter_response" "$tag")"
   local size
   size="$(file_size "$target")"
   trace "Sending $target: $(human_size "$size") ($size bytes)"
   rm -f "$response"
   run "$virtual_environment_td/bin/python" -m "$ARCHIVE_MODULE" \
      --image "$target" \
      --storage-config "$storage_config" \
      --transporter-request "$request" \
      --transporter-response "$response"
}

send_image() {
   if [[ -z "$storage_config" ]]; then
      echo ">> Skipping the image upload; no storage configuration given"
      return
   fi
   send_one_image "$output_image_td" "$VENV_TD"
   send_one_image "$output_image_fn" "$VENV_FN"
}

main() {
   local start="$SECONDS"

   parse_arguments "$@"
   validate_arguments
   setup_workspace
   step "Reporting the package indexes"       report_indexes
   step "Installing prerequisites"            install_prerequisites
   step "Installing uv"                       install_uv
   step "Installing Python"                   install_python
   step "Creating the compressor environment" create_compressor_environment
   step "Creating the virtual environments"   create_virtual_environment
   step "Installing packages"                 install_packages
   step "Pruning the virtual environments"    prune_virtual_environment
   step "Verifying the image"                 verify_image
   step "Writing the image"                   write_image
   step "Sending the image"                   send_image

   local elapsed="$(( SECONDS - start ))"
   local hours="$(awk "BEGIN { printf \"%.4f\", $elapsed / 3600 }")"
   echo ">> Done! Images written to $output_image_td and $output_image_fn"
   echo ">> Elapsed time: $hours hours"
}

main "$@"
