#
# Copyright 2024 Tabs Data Inc.
#

try:
    __import__("pkg_resources").declare_namespace(__name__)
except ImportError:
    import pkgutil

    pkgutil.extend_path(__path__, __name__)
