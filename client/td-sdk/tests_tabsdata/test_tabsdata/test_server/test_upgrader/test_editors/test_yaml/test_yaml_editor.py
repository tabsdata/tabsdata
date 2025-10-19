#
#  Copyright 2025 Tabs Data Inc.
#

from io import StringIO

import pytest
from ruamel.yaml import YAML

from tabsdata._tabsserver.server.upgrader.editors.yaml.yaml_e import YamlEditor


# noinspection PyTypeChecker
class TestYamlEditorBasics:

    def test_simple_sibling_insertion(self):
        # fmt: off
        yaml_str = \
"""\
first: value1
second: value2
third: value3
"""
        # fmt: off
        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["second"],
            "action": "insert_sibling",
            "position": "after",
            "data": {"new": "inserted"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result_yaml = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
first: value1
second: value2
new: inserted
third: value3
"""
        # fmt: on
        assert result_yaml == expected

        result = yaml.load(StringIO(result_yaml))
        keys = list(result.keys())
        assert keys == ["first", "second", "new", "third"]
        assert result["new"] == "inserted"

    def test_simple_child_insertion(self):
        # fmt: off
        yaml_str = \
"""\
parent:
  existing: value
"""
        # fmt: off
        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["parent"],
            "action": "insert_child",
            "position": "end",
            "data": {"new_child": "new_value"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result_yaml = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
parent:
  existing: value
  new_child: new_value
"""
        # fmt: on
        assert result_yaml == expected

        result = yaml.load(StringIO(result_yaml))
        assert result["parent"]["new_child"] == "new_value"
        assert result["parent"]["existing"] == "value"

    def test_multiple_transformations(self):
        # fmt: off
        yaml_str = \
"""\
section1:
  items: []
section2:
  key: value
"""
        # fmt: off
        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = [
            {
                "path": ["section1", "items"],
                "action": "insert_child",
                "position": "end",
                "data": {"id": 1},
            },
            {
                "path": ["section1", "items"],
                "action": "insert_child",
                "position": "end",
                "data": {"id": 2},
            },
            {
                "path": ["section2"],
                "action": "insert_sibling",
                "position": "after",
                "data": {"section3": {"new": "data"}},
            },
        ]

        editor = YamlEditor(spec=spec, source=source)
        result_yaml = editor.apply(dry_run=True)
        result = yaml.load(StringIO(result_yaml))

        # fmt: off
        expected = \
"""\
section1:
  items:
    - id: 1
    - id: 2
section2:
  key: value
section3:
  new: data
"""
        # fmt: on
        assert result_yaml == expected

        assert len(result["section1"]["items"]) == 2
        assert "section3" in result


# noinspection PyTypeChecker
class TestListOperations:

    def test_insert_sibling_in_list_after(self):
        # fmt: off
        yaml_str = \
"""\
workers:
  - name: worker1
    kind: processor
  - name: worker2
    kind: listener
  - name: worker3
    kind: processor
"""
        # fmt: off
        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["workers", "name:worker2"],
            "action": "insert_sibling",
            "position": "after",
            "data": {"name": "worker_new", "kind": "scheduler"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result_yaml = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
workers:
  - name: worker1
    kind: processor
  - name: worker2
    kind: listener
  - name: worker_new
    kind: scheduler
  - name: worker3
    kind: processor
"""
        # fmt: on
        assert result_yaml == expected

        workers = editor.data["workers"]

        assert len(workers) == 4
        assert workers[2]["name"] == "worker_new"
        assert workers[1]["name"] == "worker2"
        assert workers[3]["name"] == "worker3"

    def test_insert_sibling_in_list_before(self):
        # fmt: off
        yaml_str = \
"""\
workers:
  - name: worker1
    kind: processor
  - name: worker2
    kind: listener
"""
        # fmt: off
        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["workers", "name:worker2"],
            "action": "insert_sibling",
            "position": "before",
            "data": {"name": "worker_new", "kind": "scheduler"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result_yaml = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
workers:
  - name: worker1
    kind: processor
  - name: worker_new
    kind: scheduler
  - name: worker2
    kind: listener
"""
        # fmt: on
        assert result_yaml == expected

        workers = editor.data["workers"]

        assert len(workers) == 3
        assert workers[1]["name"] == "worker_new"
        assert workers[2]["name"] == "worker2"

    def test_nested_list_insertion(self):
        # fmt: off
        yaml_str = \
"""\
apps:
  - name: app1
    settings:
      timeout: 30
  - name: app2
    settings:
      timeout: 60
"""
        # fmt: off
        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["apps", "name:app2", "settings"],
            "action": "insert_child",
            "data": {"retries": 5},
        }

        editor = YamlEditor(spec=spec, source=source)
        result_yaml = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
apps:
  - name: app1
    settings:
      timeout: 30
  - name: app2
    settings:
      timeout: 60
      retries: 5
"""
        # fmt: on
        assert result_yaml == expected

        app2_settings = editor.data["apps"][1]["settings"]
        assert app2_settings["retries"] == 5
        assert app2_settings["timeout"] == 60


# noinspection PyTypeChecker
class TestCommentPreservation:

    def test_comments_preserved_in_map(self):
        # fmt: off
        yaml_str = \
"""\
# Configuration file
first: value1  # inline comment
# Comment before second
second: value2
third: value3
"""
        # fmt: off

        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["second"],
            "action": "insert_sibling",
            "position": "after",
            "data": {"new": "inserted"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
# Configuration file
first: value1  # inline comment
# Comment before second
second: value2
new: inserted
third: value3
"""
        # fmt: on
        assert result == expected

    def test_comments_preserved_in_list(self):
        # fmt: off
        yaml_str = \
"""\
# Workers configuration
workers:
  # First worker
  - name: worker1
    kind: processor
  # Second worker
  - name: worker2
    kind: listener
"""
        # fmt: off

        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["workers", "name:worker2"],
            "action": "insert_sibling",
            "position": "after",
            "data": {"name": "worker_new", "kind": "scheduler"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
# Workers configuration
workers:
  # First worker
  - name: worker1
    kind: processor
  # Second worker
  - name: worker2
    kind: listener
  - name: worker_new
    kind: scheduler
"""
        # fmt: on
        assert result == expected

    def test_insert_commented_data(self):
        original_yaml = """\
# Main config
services:
  # API service
  api:
    port: 8080
"""

        insert_yaml = """\
# Database service
database:
  host: localhost
  port: 5432
"""

        yaml = YAML()
        source = yaml.load(StringIO(original_yaml))
        insert_data = yaml.load(StringIO(insert_yaml))

        spec = {
            "path": ["services", "api"],
            "action": "insert_sibling",
            "position": "after",
            "data": insert_data,
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
# Main config
services:
  # API service
  api:
    port: 8080
  # Database service
  database:
    host: localhost
    port: 5432
"""
        # fmt: on
        assert result == expected

        assert editor.data["services"]["database"]["host"] == "localhost"
        assert editor.data["services"]["database"]["port"] == 5432
        assert "# Main config" in result
        assert "# API service" in result
        assert "database:" in result
        assert "host: localhost" in result


# noinspection PyTypeChecker
class TestRealWorldScenarios:

    def test_config_worker_insertion(self):
        # fmt: off
        yaml_str = \
"""\
name: tabsdata
controllers:
  init:
    concurrency: 1
    workers:
      - name: bootloader
        kind: processor
        location: relative
        program: bootloader
      - name: server-information
        kind: processor
        location: system
        program: tdsrvinf
"""
        # fmt: off
        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["controllers", "init", "workers", "name:bootloader"],
            "action": "insert_sibling",
            "position": "after",
            "data": {
                "name": "config-validator",
                "kind": "processor",
                "location": "relative",
                "program": "validator",
            },
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
name: tabsdata
controllers:
  init:
    concurrency: 1
    workers:
      - name: bootloader
        kind: processor
        location: relative
        program: bootloader
      - name: config-validator
        kind: processor
        location: relative
        program: validator
      - name: server-information
        kind: processor
        location: system
        program: tdsrvinf
"""
        # fmt: on
        assert result == expected

        workers = editor.data["controllers"]["init"]["workers"]
        assert len(workers) == 3
        assert workers[0]["name"] == "bootloader"
        assert workers[1]["name"] == "config-validator"
        assert workers[2]["name"] == "server-information"

    def test_config_worker_insertion_with_comments(self):
        # fmt: off
        yaml_str = \
"""\
# TabsData Configuration
name: tabsdata
controllers:
  init:
    concurrency: 1
    # Worker processes
    workers:
      # System bootstrap
      - name: bootloader
        kind: processor
        location: relative
        program: bootloader
      # Server information
      - name: server-information
        kind: processor
        location: system
        program: tdsrvinf
"""
        # fmt: off

        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["controllers", "init", "workers", "name:bootloader"],
            "action": "insert_sibling",
            "position": "after",
            "data": {
                "name": "config-validator",
                "kind": "processor",
                "location": "relative",
                "program": "validator",
            },
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        workers = editor.data["controllers"]["init"]["workers"]
        assert len(workers) == 3
        assert workers[1]["name"] == "config-validator"

        # fmt: off
        expected = \
"""\
# TabsData Configuration
name: tabsdata
controllers:
  init:
    concurrency: 1
    # Worker processes
    workers:
      # System bootstrap
      - name: bootloader
        kind: processor
        location: relative
        program: bootloader
      # Server information
      - name: config-validator
        kind: processor
        location: relative
        program: validator
      - name: server-information
        kind: processor
        location: system
        program: tdsrvinf
"""
        # fmt: on
        assert result == expected

    def test_deeply_nested_path(self):
        # fmt: off
        yaml_str = \
"""\
level1:
  level2:
    level3:
      level4:
        - name: item1
          value: 10
        - name: item2
          value: 20
"""
        # fmt: off
        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["level1", "level2", "level3", "level4", "name:item1"],
            "action": "insert_sibling",
            "position": "after",
            "data": {"name": "item1_5", "value": 15},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
level1:
  level2:
    level3:
      level4:
        - name: item1
          value: 10
        - name: item1_5
          value: 15
        - name: item2
          value: 20
"""
        # fmt: on
        assert result == expected

        items = editor.data["level1"]["level2"]["level3"]["level4"]
        assert len(items) == 3
        assert items[1]["name"] == "item1_5"
        assert items[1]["value"] == 15


class TestErrorHandling:

    def test_invalid_action_raises(self):
        # fmt: off
        yaml_str = \
"""\
key: value
"""
        # fmt: off
        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {"path": ["key"], "action": "invalid_action", "data": {"new": "value"}}

        editor = YamlEditor(spec=spec, source=source)
        with pytest.raises(ValueError, match="Unknown action"):
            editor.apply(dry_run=True)

    def test_invalid_position_raises(self):
        # fmt: off
        yaml_str = \
"""\
key: value
"""
        # fmt: off
        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["key"],
            "action": "insert_sibling",
            "position": "invalid",
            "data": {"new": "value"},
        }

        editor = YamlEditor(spec=spec, source=source)
        with pytest.raises(ValueError, match="Invalid position"):
            editor.apply(dry_run=True)

    def test_nonexistent_path_raises(self):
        # fmt: off
        yaml_str = \
"""\
existing: value
"""
        # fmt: off
        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["nonexistent"],
            "action": "insert_sibling",
            "data": {"new": "value"},
        }

        editor = YamlEditor(spec=spec, source=source)
        with pytest.raises(ValueError, match="not found"):
            editor.apply(dry_run=True)

    def test_nonexistent_list_item_raises(self):
        # fmt: off
        yaml_str = \
"""\
items:
  - id: 1
    name: first
  - id: 2
    name: second
"""
        # fmt: off
        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["items", "id:99"],
            "action": "insert_sibling",
            "data": {"id": 100},
        }

        editor = YamlEditor(spec=spec, source=source)
        with pytest.raises(ValueError, match="No item found"):
            editor.apply(dry_run=True)


# noinspection PyTypeChecker
class TestEdgeCases:

    def test_insert_at_list_start(self):
        # fmt: off
        yaml_str = \
"""\
items:
  - id: 2
  - id: 3
"""
        # fmt: off
        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["items", "id:2"],
            "action": "insert_sibling",
            "position": "before",
            "data": {"id": 1},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
items:
  - id: 1
  - id: 2
  - id: 3
"""
        # fmt: on
        assert result == expected

        items = editor.data["items"]
        assert items[0]["id"] == 1
        assert items[1]["id"] == 2

    def test_insert_at_list_end(self):
        # fmt: off
        yaml_str = \
"""\
items:
  - id: 1
"""
        # fmt: off
        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["items"],
            "action": "insert_child",
            "position": "end",
            "data": {"id": 2},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
items:
  - id: 1
  - id: 2
"""
        # fmt: on
        assert result == expected

        items = editor.data["items"]
        assert len(items) == 2
        assert items[1]["id"] == 2

    def test_empty_list_insertion(self):
        # fmt: off
        yaml_str = \
"""\
items: []
"""
        # fmt: off
        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["items"],
            "action": "insert_child",
            "position": "end",
            "data": {"id": 1},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
items:
  - id: 1
"""
        # fmt: on
        assert result == expected

        items = editor.data["items"]
        assert len(items) == 1
        assert items[0]["id"] == 1

    def test_single_dict_spec(self):
        # fmt: off
        yaml_str = \
"""\
key: value
"""
        # fmt: off
        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["key"],
            "action": "insert_sibling",
            "position": "after",
            "data": {"new": "inserted"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result_yaml = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
key: value
new: inserted
"""
        # fmt: on
        assert result_yaml == expected

        result = yaml.load(StringIO(result_yaml))
        assert "new" in result


# noinspection PyTypeChecker
class TestComplexCommentScenarios:

    def test_multiple_inline_comments(self):
        """Test preservation of multiple inline comments."""
        # fmt: off
        yaml_str = \
"""\
config:
  key1: value1  # first comment
  key2: value2  # second comment
  key3: value3  # third comment
"""
        # fmt: off

        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["config", "key2"],
            "action": "insert_sibling",
            "position": "after",
            "data": {"newkey": "newvalue"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
config:
  key1: value1  # first comment
  key2: value2  # second comment
  newkey: newvalue
  key3: value3  # third comment
"""
        # fmt: on
        assert result == expected

    def test_nested_comments_at_multiple_levels(self):
        # fmt: off
        yaml_str = \
"""\
# Top level comment
root:
  # Level 1 comment
  level1:
    # Level 2 comment
    level2:
      # Level 3 comment
      item1: value1
      item2: value2
"""
        # fmt: off

        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["root", "level1", "level2", "item1"],
            "action": "insert_sibling",
            "position": "after",
            "data": {"item1_5": "value1_5"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
# Top level comment
root:
  # Level 1 comment
  level1:
    # Level 2 comment
    level2:
      # Level 3 comment
      item1: value1
      item1_5: value1_5
      item2: value2
"""
        # fmt: on
        assert result == expected

    def test_comments_before_and_after_list_items(self):
        # fmt: off
        yaml_str = \
"""\
workers:
  # First worker
  - name: worker1
    kind: processor
  # Second worker
  - name: worker2
    kind: listener
  # Third worker
  - name: worker3
    kind: processor
"""
        # fmt: off

        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["workers", "name:worker2"],
            "action": "insert_sibling",
            "position": "before",
            "data": {"name": "worker1_5", "kind": "scheduler"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
workers:
  # First worker
  - name: worker1
    kind: processor
  # Second worker
  - name: worker1_5
    kind: scheduler
  - name: worker2
    kind: listener
  # Third worker
  - name: worker3
    kind: processor
"""
        # fmt: on
        assert result == expected

    def test_multiline_comments(self):
        # fmt: off
        yaml_str = \
"""\
# This is a configuration file
# It contains multiple settings
# Each setting is important
settings:
  # Database configuration
  # Connects to production database
  database:
    host: localhost
  # Cache configuration
  # Uses Redis for caching
  cache:
    ttl: 3600
"""
        # fmt: off

        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["settings", "database"],
            "action": "insert_sibling",
            "position": "after",
            "data": {"logging": {"level": "info"}},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # When inserting after database, the cache comment stays with cache
        # fmt: off
        expected = \
"""\
# This is a configuration file
# It contains multiple settings
# Each setting is important
settings:
  # Database configuration
  # Connects to production database
  database:
    host: localhost
  # Cache configuration
  # Uses Redis for caching
  logging:
    level: info
  cache:
    ttl: 3600
"""
        # fmt: on
        assert result == expected

    def test_empty_lines_with_comments(self):
        # fmt: off
        yaml_str = \
"""\
config:

  # First section
  section1: value1

  # Second section
  section2: value2

  # Third section
  section3: value3
"""
        # fmt: off

        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["config", "section1"],
            "action": "insert_sibling",
            "position": "after",
            "data": {"section1_5": "value1_5"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
config:

  # First section
  section1: value1

  # Second section
  section1_5: value1_5
  section2: value2

  # Third section
  section3: value3
"""
        # fmt: on
        assert result == expected

        assert "# First section" in result
        assert "# Second section" in result
        assert "# Third section" in result
        assert "section1_5: value1_5" in result

    def test_list_with_mixed_comment_styles(self):
        # fmt: off
        yaml_str = \
"""\
items:
  - id: 1  # inline comment for item 1
    name: first
  # Comment before item 2
  - id: 2
    name: second  # inline on field
  - id: 3  # inline comment for item 3
    name: third
"""
        # fmt: off

        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["items", "id:2"],
            "action": "insert_sibling",
            "position": "after",
            "data": {"id": 2.5, "name": "two-and-half"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
items:
  - id: 1  # inline comment for item 1
    name: first
  # Comment before item 2
  - id: 2
    name: second  # inline on field
  - id: 2.5
    name: two-and-half
  - id: 3  # inline comment for item 3
    name: third
"""
        # fmt: on
        assert result == expected

        assert (
            "# Comment before item 2" in result
            or "# inline comment for item 1" in result
        )
        assert "id: 2.5" in result or "id: 2" in result
        assert editor.data["items"][2]["id"] == 2.5

    def test_deeply_nested_with_comments_at_each_level(self):
        """Test deep nesting with comments at every level."""
        # fmt: off
        yaml_str = \
"""\
# Root comment
app:
  # Config comment
  config:
    # Database comment
    database:
      # Connection comment
      connection:
        # Host comment
        host: localhost
        # Port comment
        port: 5432
"""
        # fmt: off

        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["app", "config", "database", "connection", "host"],
            "action": "insert_sibling",
            "position": "after",
            "data": {"username": "admin"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
# Root comment
app:
  # Config comment
  config:
    # Database comment
    database:
      # Connection comment
      connection:
        # Host comment
        host: localhost
        # Port comment
        username: admin
        port: 5432
"""
        # fmt: on
        assert result == expected

    def test_insert_at_beginning_with_comment(self):
        # fmt: off
        yaml_str = \
"""\
config:
  # First key
  key1: value1
  key2: value2
"""
        # fmt: off

        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["config", "key1"],
            "action": "insert_sibling",
            "position": "before",
            "data": {"key0": "value0"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
config:
  # First key
  key0: value0
  key1: value1
  key2: value2
"""
        # fmt: on
        assert result == expected

    def test_insert_at_end_with_comment(self):
        # fmt: off
        yaml_str = \
"""\
config:
  key1: value1
  # Last key
  key2: value2
"""
        # fmt: off

        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["config", "key2"],
            "action": "insert_sibling",
            "position": "after",
            "data": {"key3": "value3"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
config:
  key1: value1
  # Last key
  key2: value2
  key3: value3
"""
        # fmt: on
        assert result == expected

    def test_multiple_transformations_with_comments(self):
        # fmt: off
        yaml_str = \
"""\
# Main config
services:
  # Service A
  service_a:
    port: 8080
  # Service C
  service_c:
    port: 8082
"""
        # fmt: off

        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = [
            {
                "path": ["services", "service_a"],
                "action": "insert_sibling",
                "position": "after",
                "data": {"service_b": {"port": 8081}},
            },
            {
                "path": ["services", "service_c"],
                "action": "insert_sibling",
                "position": "after",
                "data": {"service_d": {"port": 8083}},
            },
        ]

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
# Main config
services:
  # Service A
  service_a:
    port: 8080
  # Service C
  service_b:
    port: 8081
  service_c:
    port: 8082
  service_d:
    port: 8083
"""
        # fmt: on
        assert result == expected

    def test_list_insertion_preserves_surrounding_comments(self):
        # fmt: off
        yaml_str = \
"""\
# Configuration
workers:
  # Essential worker
  - name: critical
    priority: high
  # Optional worker
  - name: optional
    priority: low
"""
        # fmt: off

        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["workers", "name:critical"],
            "action": "insert_sibling",
            "position": "after",
            "data": {"name": "medium", "priority": "medium"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
# Configuration
workers:
  # Essential worker
  - name: critical
    priority: high
  # Optional worker
  - name: medium
    priority: medium
  - name: optional
    priority: low
"""
        # fmt: on
        assert result == expected

    def test_complex_nested_list_with_comments(self):
        # fmt: off
        yaml_str = \
"""\
# Application config
application:
  # Modules section
  modules:
    # Core module
    - name: core
      # Core dependencies
      dependencies:
        - lib1
        - lib2
    # Plugin module
    - name: plugin
      dependencies:
        - lib3
"""
        # fmt: off

        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["application", "modules", "name:core"],
            "action": "insert_sibling",
            "position": "after",
            "data": {"name": "extension", "dependencies": ["lib2", "lib4"]},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
# Application config
application:
  # Modules section
  modules:
    # Core module
    - name: core
      # Core dependencies
      dependencies:
        - lib1
        - lib2
    # Plugin module
    - name: extension
      dependencies:
        - lib2
        - lib4
    - name: plugin
      dependencies:
        - lib3
"""
        # fmt: on
        assert result == expected

        assert editor.data["application"]["modules"][1]["name"] == "extension"
        assert "# Application config" in result
        assert "# Modules section" in result

    def test_insert_child_in_empty_map_with_comment(self):
        # fmt: off
        yaml_str = \
"""\
config:
  # Empty section
  section: {}
"""
        # fmt: off

        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["config", "section"],
            "action": "insert_child",
            "data": {"key": "value"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
config:
  # Empty section
  section:
    key: value
"""
        # fmt: on
        assert result == expected

    def test_insert_child_in_empty_list_with_comment(self):
        # fmt: off
        yaml_str = \
"""\
config:
  # Empty list
  items: []
"""
        # fmt: off

        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["config", "items"],
            "action": "insert_child",
            "data": {"id": 1, "name": "first"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
config:
  # Empty list
  items:
    - id: 1
      name: first
"""
        # fmt: on
        assert result == expected

    def test_special_characters_in_comments(self):
        # fmt: off
        yaml_str = \
"""\
config:
  # Special chars: @#$%^&*()
  key1: value1
  # Unicode: café, 日本語, مرحبا
  key2: value2
  # Symbols: ™ © ® € £
  key3: value3
"""
        # fmt: off

        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["config", "key2"],
            "action": "insert_sibling",
            "position": "before",
            "data": {"key1_5": "value1_5"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
config:
  # Special chars: @#$%^&*()
  key1: value1
  # Unicode: café, 日本語, مرحبا
  key1_5: value1_5
  key2: value2
  # Symbols: ™ © ® € £
  key3: value3
"""
        # fmt: on
        assert result == expected

        assert "@#$%^&*()" in result
        assert "café" in result or "Unicode" in result
        assert "™" in result or "©" in result or "Symbols" in result

    def test_comment_with_yaml_like_content(self):
        # fmt: off
        yaml_str = \
"""\
config:
  # Example: key: value
  key1: value1
  # TODO: add key2: value2
  key3: value3
"""
        # fmt: off

        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["config", "key1"],
            "action": "insert_sibling",
            "position": "after",
            "data": {"key2": "value2"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
config:
  # Example: key: value
  key1: value1
  # TODO: add key2: value2
  key2: value2
  key3: value3
"""
        # fmt: on
        assert result == expected

    def test_mixed_indent_comments(self):
        # fmt: off
        yaml_str = \
"""\
root:
# Top level comment (no indent)
  level1:
    # Properly indented
    level2:
      # Also properly indented
      key: value
"""
        # fmt: off

        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {
            "path": ["root", "level1", "level2"],
            "action": "insert_child",
            "data": {"key2": "value2"},
        }

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
root:
# Top level comment (no indent)
  level1:
    # Properly indented
    level2:
      # Also properly indented
      key: value
      key2: value2
"""
        # fmt: on
        assert result == expected

        # Verify structure is maintained
        assert editor.data["root"]["level1"]["level2"]["key2"] == "value2"
        assert "key: value" in result


class TestSpecFormats:

    def test_spec_as_dict(self):
        # fmt: off
        yaml_str = \
"""\
key: value
"""
        # fmt: off
        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = {"path": ["key"], "action": "insert_sibling", "data": {"new": "data"}}

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)
        # fmt: off
        expected = \
"""\
key: value
new: data
"""
        # fmt: on
        assert result == expected
        assert "new" in result

    def test_spec_as_list(self):
        # fmt: off
        yaml_str = \
"""\
key: value
"""
        # fmt: off
        yaml = YAML()
        source = yaml.load(StringIO(yaml_str))

        spec = [
            {"path": ["key"], "action": "insert_sibling", "data": {"new1": "data1"}},
            {"path": ["new1"], "action": "insert_sibling", "data": {"new2": "data2"}},
        ]

        editor = YamlEditor(spec=spec, source=source)
        result_yaml = editor.apply(dry_run=True)

        # fmt: off
        expected = \
"""\
key: value
new1: data1
new2: data2
"""
        # fmt: on
        assert result_yaml == expected

        result = yaml.load(StringIO(result_yaml))
        assert "new1" in result
        assert "new2" in result

    def test_spec_as_commented_map(self):
        spec_yaml = """\
# Transformation spec
path: ["key"]
action: insert_sibling
data:
  new: inserted
"""

        yaml = YAML()
        spec = yaml.load(StringIO(spec_yaml))

        source_yaml = """\
key: value
"""
        source = yaml.load(StringIO(source_yaml))

        editor = YamlEditor(spec=spec, source=source)
        result = editor.apply(dry_run=True)
        # fmt: off
        expected = \
"""\
key: value
new: inserted
"""
        # fmt: on
        assert result == expected
        assert "new" in result
