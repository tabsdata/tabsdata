#!/usr/bin/env bash
#
# Copyright 2026 Tabs Data Inc.
#
# End-to-end authorization (project-oriented RBAC / Simple Security) validation
# driven entirely through the `td` CLI.
#
# It (as admin) creates projects, collections and users, grants every Simple
# permission type, then logs in as each user and runs a battery of operations,
# asserting the expected ALLOW / DENY outcome (exit code 0 == allowed).
#
# Scratch / throwaway -- NOT committed. Safe to delete.
#
# Requirements:
#   * `td` CLI on PATH (override with TD=...).
#   * A running Tabsdata apiserver. Because it creates projects (which mints the
#     per-project <name>_view/_dev/_admin roles the project grants resolve to),
#     the server must be Postgres-backed; on SQLite `project create` is rejected
#     and the script aborts during setup.
#
# Config (env, with defaults):
#   TD_SERVER          (default http://127.0.0.1:2457)
#   TD_ADMIN_USER      (default admin)
#   TD_ADMIN_PASSWORD  (default tabsdata)
#   TD                 (default td)
#
# Notes / model facts this exercises:
#   * collection list   -> requires ViewProject
#   * collection create -> requires DevelopProject
#   * permission grant (project perm) -> requires ManageProject (on that
#       project) or ManageAccess
#   * permission grant (system perm)  -> requires ManageAccess
#   * manage_access AND admin_all_projects both map to the sys_admin role
#       (full access) -- so those two users behave identically.
#   * EVERY user can create projects: the auto-assigned `default` role carries
#       `create_projects`. So "create project" is expected ALLOW for all.

set -uo pipefail

TD="${TD:-td}"
SERVER="${TD_SERVER:-http://127.0.0.1:2457}"
ADMIN_USER="${TD_ADMIN_USER:-admin}"
ADMIN_PW="${TD_ADMIN_PASSWORD:-tabsdata}"
INIT_PW="authz_init_1"                # password at create time (must be changed)
PW="authz_pw_1"                       # working password after the forced change
SFX="$$_${RANDOM}"                  # unique-per-run suffix

PROJ_A="authz_pa_${SFX}"
PROJ_B="authz_pb_${SFX}"

U_VIEW="authz_view_${SFX}"
U_DEV="authz_dev_${SFX}"
U_MGR="authz_mgr_${SFX}"
U_CREATOR="authz_creator_${SFX}"
U_ACCESS="authz_access_${SFX}"
U_ADMINALL="authz_adminall_${SFX}"
U_NONE="authz_none_${SFX}"
U_TARGET="authz_target_${SFX}"        # grant target; never itself validated
ALL_USERS=("$U_VIEW" "$U_DEV" "$U_MGR" "$U_CREATOR" "$U_ACCESS" "$U_ADMINALL" "$U_NONE" "$U_TARGET")

PASS=0
FAIL=0
TOTAL=0

# --------------------------------------------------------------------------- #
# Session isolation: `td` stores credentials at ~/.tabsdata/connection.json
# (not env-overridable), so logging in here would clobber your existing session.
# Back it up and restore on exit.
# --------------------------------------------------------------------------- #
TD_DIR="${HOME}/.tabsdata"
CONN="${TD_DIR}/connection.json"
PINNED="${TD_DIR}/pinned.json"
BK="$(mktemp -d)"
[ -f "$CONN" ]   && cp "$CONN"   "$BK/connection.json" 2>/dev/null || true
[ -f "$PINNED" ] && cp "$PINNED" "$BK/pinned.json"     2>/dev/null || true

login_as() { "$TD" --no-prompt login --server "$SERVER" --user "$1" --password "$2" >/dev/null 2>&1; }
as_admin() { login_as "$ADMIN_USER" "$ADMIN_PW"; }

q() { "$TD" --no-prompt "$@" >/dev/null 2>&1; }   # quiet td call

cleanup() {
    echo
    echo "── cleanup ──"
    as_admin || true
    "$TD" --no-prompt permission revoke --user "$U_TARGET" --permission view_project --project "$PROJ_A" >/dev/null 2>&1 || true
    "$TD" --no-prompt permission revoke --user "$U_TARGET" --permission manage_access >/dev/null 2>&1 || true
    for u in "${ALL_USERS[@]}"; do
        "$TD" --no-prompt user delete -n "$u" --confirm delete >/dev/null 2>&1 || true
    done
    # Deleting the projects cascades their collections and per-project roles.
    "$TD" --no-prompt project delete -n "$PROJ_A" --confirm delete >/dev/null 2>&1 || true
    "$TD" --no-prompt project delete -n "$PROJ_B" --confirm delete >/dev/null 2>&1 || true
    # restore the caller's session
    if [ -f "$BK/connection.json" ]; then cp "$BK/connection.json" "$CONN" 2>/dev/null || true; else rm -f "$CONN" 2>/dev/null || true; fi
    if [ -f "$BK/pinned.json" ];     then cp "$BK/pinned.json"     "$PINNED" 2>/dev/null || true; else rm -f "$PINNED" 2>/dev/null || true; fi
    rm -rf "$BK"
    echo "done."
}
trap cleanup EXIT

die() { echo "ERROR: $*" >&2; exit 1; }

# `record WANT GOT LABEL [DETAIL]` -- tally one assertion as PASS / FAIL.
record() {
    local want="$1" got="$2" label="$3" detail="${4:-}"
    TOTAL=$((TOTAL + 1))
    if [ "$got" = "$want" ]; then
        PASS=$((PASS + 1))
        printf "  [PASS] %-26s want=%-5s got=%-5s\n" "$label" "$want" "$got"
    else
        FAIL=$((FAIL + 1))
        printf "  [FAIL] %-26s want=%-5s got=%-5s :: %s\n" "$label" "$want" "$got" "$detail"
    fi
}

# `expect WANT LABEL <td args...>` -- runs `td --no-prompt <args>`, maps exit
# code 0 -> ALLOW else DENY, compares to WANT.
expect() {
    local want="$1" label="$2"; shift 2
    local out rc got
    out="$("$TD" --no-prompt "$@" 2>&1)"; rc=$?
    if [ "$rc" -eq 0 ]; then got="ALLOW"; else got="DENY"; fi
    record "$want" "$got" "$label" "$(printf '%s' "$out" | tr '\n' ' ' | tail -c 160)"
}

# `check_projects_visible WANT_A WANT_B` -- runs `td project list` and asserts
# whether PROJ_A / PROJ_B appear (SEE | HIDE). `project list` succeeds for any
# user (the create_projects gate, held by everyone via `default`); the result is
# then filtered to the projects the user may view, so this validates that
# project listing only surfaces projects the user has access to.
check_projects_visible() {
    local want_a="$1" want_b="$2"
    local out got_a got_b
    out="$("$TD" --no-prompt project list 2>&1)"
    if printf '%s' "$out" | grep -qF "$PROJ_A"; then got_a="SEE"; else got_a="HIDE"; fi
    if printf '%s' "$out" | grep -qF "$PROJ_B"; then got_b="SEE"; else got_b="HIDE"; fi
    record "$want_a" "$got_a" "project list: sees A" "$(printf '%s' "$out" | tr '\n' ' ' | tail -c 160)"
    record "$want_b" "$got_b" "project list: sees B" "$(printf '%s' "$out" | tr '\n' ' ' | tail -c 160)"
}

# --------------------------------------------------------------------------- #
# Setup (as admin)
# --------------------------------------------------------------------------- #
echo "=== authz validation against ${SERVER} (run ${SFX}) ==="
echo "── setup (as ${ADMIN_USER}) ──"
as_admin || die "cannot login as ${ADMIN_USER} on ${SERVER}"

q project create --name "$PROJ_A" --description "authz A" \
    || die "project create failed (server must be Postgres-backed for project-scoped tests)"
q project create --name "$PROJ_B" --description "authz B" \
    || die "project create failed for ${PROJ_B}"
echo "  created projects: ${PROJ_A}, ${PROJ_B}"

q --project "$PROJ_A" collection create --name "coll_a_${SFX}" --description "seed" \
    || die "collection create failed in ${PROJ_A}"
q --project "$PROJ_B" collection create --name "coll_b_${SFX}" --description "seed" \
    || die "collection create failed in ${PROJ_B}"
echo "  seeded a collection in each project"

for u in "${ALL_USERS[@]}"; do
    q user create --name "$u" --password "$INIT_PW" --full-name "$u" \
        || die "user create failed for ${u}"
done
echo "  created users: ${ALL_USERS[*]}"

# New users are created with password_must_change=true, so a plain login is
# rejected ("Authentication failed") until the password is changed. Run the
# forced first-change (the /auth/password_change endpoint needs no session).
for u in "${ALL_USERS[@]}"; do
    q auth password-change "$SERVER" --user "$u" --old-password "$INIT_PW" --new-password "$PW" \
        || die "forced password-change failed for ${u}"
done
echo "  performed forced password change for all users"

# Grant every permission type.
q permission grant --user "$U_VIEW"    --permission view_project    --project "$PROJ_A" || die "grant view_project failed"
q permission grant --user "$U_DEV"     --permission develop_project --project "$PROJ_A" || die "grant develop_project failed"
q permission grant --user "$U_MGR"     --permission manage_project  --project "$PROJ_A" || die "grant manage_project failed"
q permission grant --user "$U_CREATOR" --permission create_projects                     || die "grant create_projects failed"
q permission grant --user "$U_ACCESS"  --permission manage_access                       || die "grant manage_access failed"
q permission grant --user "$U_ADMINALL" --permission admin_all_projects                 || die "grant admin_all_projects failed"
echo "  granted: view/develop/manage_project@${PROJ_A}, create_projects, manage_access, admin_all_projects"
echo

echo "── granted permissions (per user) ──"
for u in "$U_VIEW" "$U_DEV" "$U_MGR" "$U_CREATOR" "$U_ACCESS" "$U_ADMINALL" "$U_NONE"; do
    echo "  - ${u}:"
    "$TD" --no-prompt permission list --user "$u" 2>&1 | sed 's/^/      /'
done

# --------------------------------------------------------------------------- #
# Validation: act as each user and assert ALLOW / DENY.
#   args: user  LIST_A CREATE_A LIST_B GRANT_PROJ_A GRANT_SYS PROJ_CREATE SEE_A SEE_B
# --------------------------------------------------------------------------- #
validate_user() {
    local user="$1" e_lista="$2" e_createa="$3" e_listb="$4" e_grantpa="$5" e_grantsys="$6" e_projcreate="$7" e_seea="$8" e_seeb="$9"
    echo
    echo "── as ${user} ──"
    login_as "$user" "$PW" || { echo "  [FAIL] login failed for ${user}"; FAIL=$((FAIL+1)); TOTAL=$((TOTAL+1)); return; }

    check_projects_visible "$e_seea" "$e_seeb"
    expect "$e_lista"     "list collections @A" --project "$PROJ_A" collection list
    expect "$e_createa"   "create collection @A" --project "$PROJ_A" collection create --name "coll_${user}" --description d
    expect "$e_listb"     "list collections @B" --project "$PROJ_B" collection list
    expect "$e_grantpa"   "grant view_proj @A"  permission grant --user "$U_TARGET" --permission view_project --project "$PROJ_A"
    expect "$e_grantsys"  "grant manage_access" permission grant --user "$U_TARGET" --permission manage_access
    expect "$e_projcreate" "create project"     project create --name "proj_${user}"

    # Reset side effects (as admin) so the next user starts clean.
    as_admin
    q permission revoke --user "$U_TARGET" --permission view_project --project "$PROJ_A" || true
    q permission revoke --user "$U_TARGET" --permission manage_access                    || true
    q --project "$PROJ_A" collection delete --name "coll_${user}" --confirm delete       || true
    q project delete --name "proj_${user}" --confirm delete                              || true
}

echo
echo "=== validation matrix ==="
echo "    (LIST_A=view@A  CREATE_A=develop@A  LIST_B=view@B  GRANT_PA=manage@A  GRANT_SYS=manage_access  PROJ_CREATE=create_projects)"
echo "    (project list: SEE_A/SEE_B = whether 'td project list' surfaces project A/B to that user)"

#              user           LIST_A CREATE_A LIST_B GRANT_PA GRANT_SYS PROJ_CREATE SEE_A SEE_B
validate_user "$U_VIEW"      ALLOW  DENY     DENY   DENY     DENY      ALLOW       SEE   HIDE
validate_user "$U_DEV"       ALLOW  ALLOW    DENY   DENY     DENY      ALLOW       SEE   HIDE
validate_user "$U_MGR"       ALLOW  ALLOW    DENY   ALLOW    DENY      ALLOW       SEE   HIDE
validate_user "$U_CREATOR"   DENY   DENY     DENY   DENY     DENY      ALLOW       HIDE  HIDE
validate_user "$U_ACCESS"    ALLOW  ALLOW    ALLOW  ALLOW    ALLOW     ALLOW       SEE   SEE
validate_user "$U_ADMINALL"  ALLOW  ALLOW    ALLOW  ALLOW    ALLOW     ALLOW       SEE   SEE
validate_user "$U_NONE"      DENY   DENY     DENY   DENY     DENY      ALLOW       HIDE  HIDE

# --------------------------------------------------------------------------- #
echo
echo "=== summary: ${PASS}/${TOTAL} passed, ${FAIL} failed ==="
[ "$FAIL" -eq 0 ]
