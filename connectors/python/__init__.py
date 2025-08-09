#
#  Copyright 2025 Tabs Data Inc.
#

# ------------------------------------------------------------------------------
# ⚠️️️️️️️️⚠️️️️️️️️⚠️️️️️️️️️ DO NOT REMOVE THIS FILE!!!
#
# This seemingly harmless __init__.py is the unsung hero that keeps pytest test
# runs happy when executed under `cargo make`. Without it, Python’s import
# machinery may suddenly "forget" how to find certain connectors’ modules.
#
# The result? Unexplained `ModuleNotFoundError` or import failures deep inside
# connector tests, despite sys.path looking perfectly fine.
#
# We could write a ten-paragraph essay on Python package discovery, namespace
# packages, and the quirky interaction with cargo-make’s test runners… but let’s
# just say: removing this file invites subtle chaos, and your future self will
# spend an afternoon chasing ghosts.
#
# TL;DR — Leave it be, and sleep better at night.
# ------------------------------------------------------------------------------
