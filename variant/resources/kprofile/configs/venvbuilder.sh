#!/usr/bin/env bash
#
# Copyright 2026 Tabsdata Inc.
#

set -euo pipefail

die() {
   echo "$1 failed" >&2
   exit 1
}

PYTHON_VERSION="3.12.13"

PYTHON_MAJOR="3"
PYTHON_MINOR="12"
PYTHON_MINIMUM_PATCH="11"

BUNDLE_NAME="tdvenvs"

UV_HOME="$HOME/.uv"
UV_PYTHON_HOME="$UV_HOME/python"
UV_VENVS_HOME="$UV_HOME/venvs"

VENV_TD="td"
VENV_AG="ag"

show_usage() {
   echo "Usage: $0 [options]"
   echo "  -p, --python VERSION           Python version                   (default: $PYTHON_VERSION)"
   echo "      --requirements-td FILE     Complementary packages for td    (default: none)"
   echo "      --requirements-ag FILE     Complementary packages for ag    (default: none)"
   echo "  -o, --output FOLDER            Output folder                    (mandatory)"
   echo "  -h, --help                     Show this help"
}

parse_arguments() {
   python_version="$PYTHON_VERSION"
   requirements_td=""
   requirements_ag=""
   output=""
   while [[ $# -gt 0 ]]; do
      case "$1" in
         -p|--python)         python_version="$2";  shift 2 ;;
         --requirements-td)   requirements_td="$2"; shift 2 ;;
         --requirements-ag)   requirements_ag="$2"; shift 2 ;;
         -o|--output)         output="$2";          shift 2 ;;
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
   if [[ -n "$requirements_ag" && ! -f "$requirements_ag" ]]; then
      echo "Requirements file for ag not found: $requirements_ag" >&2
      exit 1
   fi

   if [[ -z "$output" ]]; then
      echo "Output folder is mandatory" >&2
      show_usage
      exit 2
   fi
}

setup_workspace() {
   work="$(mktemp -d "${TMPDIR:-/tmp}/tdvenvs.XXXXXX")"
   output_bundle="$output/$BUNDLE_NAME.tar.gz"
   virtual_environment_td="$UV_VENVS_HOME/$VENV_TD"
   virtual_environment_ag="$UV_VENVS_HOME/$VENV_AG"
   export UV_PYTHON_INSTALL_DIR="$UV_PYTHON_HOME"
   echo ">> Using Python $python_version in work folder $work"
}

install_prerequisites() {
   echo ">> Installing prerequisites"
   export DEBIAN_FRONTEND=noninteractive
   apt-get -o Acquire::Retries=5 -o Acquire::http::Timeout=30 update
   apt-get -o Acquire::Retries=5 -o Acquire::http::Timeout=30 install --yes curl ca-certificates
}

install_uv() {
   echo ">> Installing uv"
   curl -LsSf https://astral.sh/uv/install.sh | sh
   export PATH="$HOME/.local/bin:$PATH"
   uv --version
}

install_python() {
   echo ">> Installing Python $python_version"
   uv python install "$python_version"
   uv python find "$python_version"
}

create_virtual_environment() {
   echo ">> Creating relocatable virtual environment $virtual_environment_td"
   uv venv --seed --relocatable --python-preference only-managed --python "$python_version" "$virtual_environment_td"
   uv pip install --python "$virtual_environment_td/bin/python" --upgrade pip
   echo ">> Creating relocatable virtual environment $virtual_environment_ag"
   uv venv --seed --relocatable --python-preference only-managed --python "$python_version" "$virtual_environment_ag"
   uv pip install --python "$virtual_environment_ag/bin/python" --upgrade pip
}

install_packages() {
   echo ">> Installing packages into virtual environment $virtual_environment_td"
   if [[ -n "$requirements_td" ]]; then
      uv pip install --python "$virtual_environment_td/bin/python" "tabsdata[all]" -r "$requirements_td"
   else
      uv pip install --python "$virtual_environment_td/bin/python" "tabsdata[all]"
   fi
   echo ">> Installing packages into virtual environment $virtual_environment_ag"
   if [[ -n "$requirements_ag" ]]; then
      uv pip install --python "$virtual_environment_ag/bin/python" "tabsdata[all]" tabsdata-agent -r "$requirements_ag"
   else
      uv pip install --python "$virtual_environment_ag/bin/python" "tabsdata[all]" tabsdata-agent
   fi
}

assemble_bundle() {
   echo ">> Assembling self-contained bundle under $UV_HOME"
   test -d "$UV_PYTHON_HOME"
   test -d "$virtual_environment_td"
   test -d "$virtual_environment_ag"
}

write_bundle() {
   echo ">> Writing bundle $output_bundle"
   mkdir -p "$output"
   tar -czf "$output_bundle" -C "$UV_HOME" python venvs
   rm -rf "$work"
}

main() {
   local start="$SECONDS"

   parse_arguments "$@"
   validate_arguments
   setup_workspace
   install_prerequisites      || die "Installing prerequisites"
   install_uv                 || die "Installing uv"
   install_python             || die "Installing Python"
   create_virtual_environment || die "Creating the virtual environments"
   install_packages           || die "Installing packages"
   assemble_bundle            || die "Assembling the bundle"
   write_bundle               || die "Writing the bundle"

   local elapsed="$(( SECONDS - start ))"
   local hours="$(awk "BEGIN { printf \"%.4f\", $elapsed / 3600 }")"
   echo ">> Done! Bundle written to $output_bundle"
   echo ">> Elapsed time: $hours hours"
}

main "$@"
