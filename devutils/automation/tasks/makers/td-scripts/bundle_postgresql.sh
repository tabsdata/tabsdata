#!/bin/sh

#
# Copyright 2026 Tabsdata Inc.
#

# Make a theseus-rs PostgreSQL tarball self-contained.
#
# Runs inside an AlmaLinux 9.6 container, chosen to match the glibc 2.34
# server floor exactly: its libxml2 carries no ICU dependency, so no second
# GLIBCXX floor appears, and it ships OpenSSL 3 (UBI 8 is disqualified — it
# has 1.1.1, while PostgreSQL 18 needs libssl.so.3). Produces a tree that
# starts on every glibc >= 2.34 distro without the host providing anything but
# libc/libm/libresolv.
#
# Two defects in the upstream artifact make this necessary:
#
#   1. lib/libpq.so.5 carries RUNPATH /opt/postgresql/lib — the theseus build
#      machine's path, absent everywhere else. DT_RUNPATH is NOT inherited by
#      dependencies, so although bin/* correctly say $ORIGIN/../lib, libpq's
#      OWN deps are never searched in the bundle. Copying libs in without
#      patching therefore changes nothing: verified, libssl.so.3 sat in lib/
#      and the loader still reported it missing.
#
#   2. The missing libs differ per distro (2 on RHEL 9, 8 on Debian 12), so
#      the union is bundled and the same tree ships everywhere.
#
# Inputs:  $1 = postgres tree root (contains bin/ and lib/)
# Output:  the same tree, self-contained, patched in place.

set -e

# Bump on any change to what this script produces. cache_postgresql.py refuses
# a bundle stamped with a different version and rebuilds it.
BUNDLER_VERSION=1
MARKER_FILE=.td-bundled

LEAK_REPORT=/tmp/td-postgresql-leaks.txt

# ------------------------------------------------------------------------------
# Library classification
# ------------------------------------------------------------------------------

# Host-provided, never bundled.
#
# libc/libm/libpthread/libdl/librt/libresolv/libutil ARE the ABI floor: they
# must come from the running system, and bundling them is what breaks a build.
#
# libstdc++/libgcc_s are excluded for the same reason, learned the hard way:
# collecting them from Debian 12 (glibc 2.36) produced a bundle that failed on
# RHEL 9 with `GLIBC_2.36 not found (required by libstdc++.so.6)`. They are
# toolchain runtime, so the host's copy is the correct one.
is_host_lib() {
  case "$1" in
    libc.so.6|libm.so.6|libpthread.so.0|libdl.so.2|librt.so.1|ld-linux*|\
    libresolv.so.2|libutil.so.1|libstdc++.so.6|libgcc_s.so.1) return 0 ;;
  esac
  return 1
}

# libreadline is GPLv3+ and libtinfo comes with it. Neither is bundled, so the
# shipped artifact has no GPL contact at all — PostgreSQL itself is under the
# permissive PostgreSQL License, and the pristine theseus tree contains no GPL
# text anywhere.
is_excluded_lib() {
  case "$1" in libreadline.so.*|libtinfo.so.*) return 0 ;; esac
  return 1
}

# PL/Python is an OPTIONAL procedural-language module: it is loaded only by
# `CREATE EXTENSION plpython3u`, never by the server at startup, and links the
# interpreter of whatever machine built it (libpython3.11). Bundling a CPython
# would drag a second interpreter into the artifact for a feature tabsdata does
# not use, so these are skipped rather than resolved. The server starts and
# runs with them present-but-unloadable.
is_optional_module() {
  case "$1" in *_plpython3.so|plpython3.so) return 0 ;; esac
  return 1
}

needed_libs() {
  objdump -p "$1" 2>/dev/null | awk '/NEEDED/{print $2}'
}

# ------------------------------------------------------------------------------
# Preconditions
# ------------------------------------------------------------------------------

require_tools() {
  # Checked up front and fatally: patchelf lives in EPEL on the RHEL rebuilds,
  # so a plain `dnf install patchelf` silently installs nothing. Without it the
  # RUNPATH rewrite is a no-op and the bundle looks built but resolves against
  # the host — the exact failure this script exists to prevent.
  for tool in patchelf objdump ldd; do
    command -v "$tool" >/dev/null 2>&1 || {
      echo "::error::$tool is required (patchelf is in EPEL on RHEL rebuilds)"
      exit 1
    }
  done
}

resolve_root() {
  [ -d "$1/bin" ] && [ -d "$1/lib" ] || {
    echo "::error::$1 is not a postgres tree (no bin/ and lib/)"
    exit 1
  }
  # Normalised to an ABSOLUTE path before anything compares against it. `ldd`
  # always prints absolute paths, so a relative root (which is what a caller
  # passing "pgroot/postgresql-..." gives) makes the self-containment check
  # below match nothing and report every correctly-bundled library as a host
  # leak — 223 false failures.
  ( cd "$1" && pwd )
}

library_search_path() {
  for dir in /usr/lib64 /lib64 /usr/lib/x86_64-linux-gnu /lib/x86_64-linux-gnu \
             /usr/lib/aarch64-linux-gnu /lib/aarch64-linux-gnu /usr/lib; do
    [ -d "$dir" ] && printf '%s ' "$dir"
  done
}

# ------------------------------------------------------------------------------
# Bundling steps
# ------------------------------------------------------------------------------

# psql is REMOVED rather than shipped unusable.
#
# It is the only binary in the tree that links readline (verified: the other 36
# are clean), so keeping it would mean either bundling GPLv3 or shipping a
# binary that dies with a loader error on most hosts. Nothing in tabsdata
# invokes psql — os, ee and km were all searched — and the embedded server's
# password is generated in-process, so the documented way to inspect data is the
# apiserver API or `tdk`, never psql. For the rare manual case, a system psql
# (`apt install postgresql-client`) talks to the embedded server fine.
remove_psql() {
  echo "--- removing psql (GPLv3 readline; unused by tabsdata) ---"
  if [ -e "$ROOT/bin/psql" ]; then
    rm -f "$ROOT/bin/psql"
    echo "  removed bin/psql"
  else
    echo "  bin/psql already absent"
  fi
}

# Walked from ELF NEEDED entries, NOT from ldd.
#
# `ldd` resolves against the BUILD host, which has these libs installed, so it
# reports nothing missing and silently omits them from the bundle. That is a
# real bug that was hit: the collector printed "added 0" while libkrb5.so.3 was
# absent, and the failure only appeared later on a distro without it.
collect_closure() {
  echo "--- collecting the NEEDED closure ---"
  search=$(library_search_path)
  round=0
  while [ "$round" -lt 8 ]; do
    round=$((round + 1))
    added=0
    for file in "$ROOT"/bin/* "$ROOT"/lib/*.so*; do
      [ -f "$file" ] || continue
      for need in $(needed_libs "$file"); do
        is_host_lib "$need" && continue
        is_excluded_lib "$need" && continue
        [ -e "$ROOT/lib/$need" ] && continue
        for dir in $search; do
          if [ -e "$dir/$need" ]; then
            cp -L "$dir/$need" "$ROOT/lib/"
            echo "    + $need"
            added=$((added + 1))
            break
          fi
        done
      done
    done
    [ "$added" -eq 0 ] && break
  done
  echo "  closure converged after $round round(s)"
}

# The fix that actually makes the bundle load. $ORIGIN (not $ORIGIN/../lib):
# everything here lives IN lib/, so each file finds its siblings beside itself,
# wherever the tree is installed.
#
# This covers BOTH kinds of file in lib/, which share the same defect:
#   * the shared libraries (libpq.so.5 ships RUNPATH /opt/postgresql/lib)
#   * the contrib extension modules that postgres dlopens — pgxml.so,
#     sslinfo.so, uuid-ossp.so, postgres_fdw.so ... — which carry that same dead
#     path. Patching only the libraries leaves every CREATE EXTENSION resolving
#     against the host.
patch_runpaths() {
  echo "--- patching RUNPATH to \$ORIGIN on everything in lib/ ---"
  patched=0
  failed=0
  for file in "$ROOT"/lib/*.so*; do
    [ -f "$file" ] || continue
    # Skip anything that is not ELF (lib/ also holds .a, .pc and share data).
    head -c 4 "$file" 2>/dev/null | grep -q 'ELF' || continue
    if patchelf --set-rpath '$ORIGIN' "$file" 2>/dev/null; then
      patched=$((patched + 1))
    else
      echo "  WARN could not patch $(basename "$file")"
      failed=$((failed + 1))
    fi
  done
  echo "  patched $patched files ($failed failed)"
  [ "$patched" -gt 0 ] || {
    echo "::error::patched nothing; patchelf is not working"
    exit 1
  }
}

# ------------------------------------------------------------------------------
# Assertions
# ------------------------------------------------------------------------------

# Fail the build here rather than on a user's machine: anything resolving
# outside the bundle other than the host libs is a packaging bug, and it would
# otherwise pass CI on distros that happen to carry the library and fail only in
# the field. This is the check that catches a rename like Ubuntu 26.04's
# libxml2.so.2 -> libxml2.so.16.
assert_self_contained() {
  echo "--- self-containment check ---"
  for file in "$ROOT"/bin/postgres "$ROOT"/bin/initdb "$ROOT"/lib/*.so*; do
    [ -f "$file" ] || continue
    is_optional_module "$(basename "$file")" && continue
    ldd "$file" 2>/dev/null | while read -r line; do
      name=$(echo "$line" | awk '{print $1}')
      case "$name" in lib*) ;; *) continue ;; esac
      is_host_lib "$name" && continue
      # "libfoo.so => not found" must be read from the whole line: field 3 is
      # the word "not", which would otherwise be mistaken for a resolved path.
      case "$line" in
        *"not found"*)
          echo "  UNRESOLVED $name (from $(basename "$file"))"
          continue
          ;;
      esac
      path=$(echo "$line" | awk '{print $3}')
      case "$path" in
        "$ROOT"/*) ;;
        "") ;;
        *) echo "  HOST-LEAK   $name -> $path (from $(basename "$file"))" ;;
      esac
    done
  done > "$LEAK_REPORT"
  leaked=$(wc -l < "$LEAK_REPORT" | tr -d ' ')
  if [ "$leaked" -ne 0 ]; then
    cat "$LEAK_REPORT"
    echo "::error::postgres bundle is not self-contained ($leaked entries)"
    exit 1
  fi
  echo "  self-contained: only libc/libm/libresolv come from the host"
}

# Guards the licensing decision itself, so a future postgres version that adds
# another readline-linking binary cannot quietly reintroduce the GPLv3
# dependency (or ship something that cannot load).
assert_no_gpl() {
  echo "--- GPL check: nothing may link readline ---"
  gpl=0
  for file in "$ROOT"/bin/* "$ROOT"/lib/*.so*; do
    [ -f "$file" ] || continue
    if needed_libs "$file" | grep -qE 'libreadline|libtinfo'; then
      echo "  $(basename "$file") links readline/tinfo"
      gpl=$((gpl + 1))
    fi
  done
  if [ "$gpl" -ne 0 ]; then
    echo "::error::$gpl file(s) link GPLv3 readline; remove them or reconsider the licensing"
    exit 1
  fi
  echo "  clean: no GPLv3 dependency in the tree"
}

# ------------------------------------------------------------------------------
# Reporting
# ------------------------------------------------------------------------------

max_symbol_version() {
  for file in "$ROOT"/bin/* "$ROOT"/lib/*.so*; do
    objdump -T "$file" 2>/dev/null | grep -oE "$1"
  done | sort -uV | tail -1
}

count_libs() {
  ls "$ROOT"/lib/*.so* 2>/dev/null | wc -l | tr -d ' '
}

report_floors() {
  echo "--- bundle floors ---"
  MAX_GLIBC=$(max_symbol_version 'GLIBC_[0-9]+\.[0-9]+')
  max_cxx=$(max_symbol_version 'GLIBCXX_[0-9]+\.[0-9]+\.[0-9]+')
  echo "  max GLIBC_:   ${MAX_GLIBC:-none}"
  echo "  max GLIBCXX_: ${max_cxx:-none (no C++ in the bundle)}"
  echo "  libs in lib/: $(count_libs)"
}

# The marker the cache task looks for. Bundled and unbundled archives are
# otherwise indistinguishable -- same filename, same layout -- so without it a
# previously cached (unbundled) archive is silently shipped.
#
# BUNDLER_VERSION must be bumped whenever this script changes what it produces
# (a new library, different patching, another exclusion). The cache task
# re-bundles anything stamped with an older version, so an archive cached by a
# previous bundler is never mistaken for a current one.
write_marker() {
  cat > "$ROOT/$MARKER_FILE" <<MARKER
bundler-version=${BUNDLER_VERSION}
glibc-floor=${MAX_GLIBC:-unknown}
libs=$(count_libs)
MARKER
  echo "  marker written: $MARKER_FILE (bundler-version=${BUNDLER_VERSION})"
}

# ------------------------------------------------------------------------------

main() {
  ROOT=$(resolve_root "${1:?usage: bundle_postgresql.sh <postgres-root>}")
  require_tools

  remove_psql
  collect_closure
  patch_runpaths

  assert_self_contained
  assert_no_gpl

  report_floors
  write_marker
}

main "$@"
