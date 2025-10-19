#
#  Copyright 2025 Tabs Data Inc.
#

from copy import deepcopy
from io import StringIO
from pathlib import Path
from typing import Any, Literal, Optional, Union, get_args

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq

SiblingPosition = Literal["after", "before"]
ChildPosition = Literal["end", "start"]
Action = Literal["insert_sibling", "insert_child"]


class YamlEditor:
    def __init__(
        self,
        spec: Union[str, Path, dict, list, CommentedMap, CommentedSeq],
        source: Union[str, Path, dict, CommentedMap, CommentedSeq],
        *,
        output: Optional[Union[str, Path]] = None,
    ):
        self.yaml = YAML()
        self.yaml.preserve_quotes = True
        self.yaml.indent(mapping=2, sequence=4, offset=2)
        self.yaml.width = 4096
        if isinstance(spec, (str, Path)):
            spec_path = Path(spec)
            if not spec_path.exists():
                raise FileNotFoundError(f"Spec file not found: {spec_path}")
            with spec_path.open("r", encoding="utf-8") as f:
                self.spec_data = self.yaml.load(f)
        elif isinstance(spec, (dict, list, CommentedMap, CommentedSeq)):
            self.path = None
            self.spec_data = spec
        else:
            raise ValueError(f"Invalid spec type: {type(spec)}")
        if not isinstance(self.spec_data, list):
            self.spec_data = [self.spec_data]
        if isinstance(source, (str, Path)):
            self.path = Path(source)
            if not self.path.exists():
                raise FileNotFoundError(f"Source file not found: {self.path}")
            with self.path.open("r", encoding="utf-8") as f:
                self.data = self.yaml.load(f)
        elif isinstance(source, (CommentedMap, CommentedSeq)):
            self.path = None
            self.data = source
        else:
            raise ValueError(
                f"Invalid source type: {type(source)}. Use file path or"
                " CommentedMap/CommentedSeq"
            )

        self.output = Path(output) if output else None

    @staticmethod
    def copy_comments_from_source(
        source: Any,
        destination: CommentedMap,
        key: str,
        indent: int,
    ) -> None:
        if not isinstance(destination, CommentedMap):
            return
        if not hasattr(source, "ca"):
            return
        source_items = getattr(source.ca, "items", None)
        if source_items and key in source_items:
            destination.ca.items[key] = deepcopy(source_items[key])
        comment_attr = getattr(source.ca, "comment", None)
        if comment_attr and len(comment_attr) > 1 and comment_attr[1]:
            lines: list[str] = []
            for token in comment_attr[1]:
                value = getattr(token, "value", "")
                value = value.rstrip("\n")
                if value.startswith("#"):
                    value = value[1:]
                    if value.startswith(" "):
                        value = value[1:]
                lines.append(value)
            before_comment = "\n".join(lines).rstrip("\n")
            if before_comment:
                destination.yaml_set_comment_before_after_key(
                    key, before=before_comment, indent=indent
                )

    def navigate_route(  # noqa: C901
        self, route: list[str]
    ) -> tuple[Any | None, Any, Union[str, tuple[str, str], None]]:
        def resolve_node(segment: Any, node: str) -> Any:
            if ":" in node:
                key, value = node.split(":", 1)
                if not isinstance(segment, list):
                    raise ValueError(
                        f"Cannot search with '{node}' in non-list type {type(segment)}"
                    )
                for element in segment:
                    if isinstance(element, dict):
                        element_value = element.get(key)
                        if element_value == value or str(element_value) == value:
                            return element
                raise ValueError(f"No item found with {key}={value}")
            else:
                if isinstance(segment, dict):
                    if node not in segment:
                        raise ValueError(f"Key '{node}' not found in dict")
                    return segment[node]
                elif isinstance(segment, list):
                    for element in segment:
                        if isinstance(element, dict) and node in element:
                            return element
                    raise ValueError(
                        f"No dict item found in list containing key '{node}'"
                    )
                else:
                    raise ValueError(
                        f"Cannot access key '{node}' in non-dict type {type(segment)}"
                    )

        if not route:
            return None, self.data, None
        parent = self.data
        for i, item in enumerate(route[:-1]):
            parent = resolve_node(parent, item)
        destination = route[-1]
        target = resolve_node(parent, destination)
        if ":" in destination:
            anchor = tuple(destination.split(":", 1))
        else:
            if isinstance(parent, list) and isinstance(target, dict):
                anchor = (destination, target.get(destination))
            else:
                anchor = destination
        return parent, target, anchor

    # noinspection DuplicatedCode
    def insert_sibling(  # noqa: C901
        self, route: list[str], data: Any, position: SiblingPosition = "after"
    ) -> None:
        if position not in get_args(SiblingPosition):
            raise ValueError(
                f"Invalid position '{position}'. Must be one of"
                f" {get_args(SiblingPosition)}"
            )
        parent, target, anchor = self.navigate_route(route)
        if isinstance(parent, CommentedMap):
            if not isinstance(anchor, str):
                raise ValueError(f"Expected string key for map, got {type(anchor)}")
            keys = list(parent.keys())
            anchor_index = keys.index(anchor)
            if position == "after":
                data_index = anchor_index + 1
            else:
                data_index = anchor_index
            if isinstance(data, dict) and len(data) == 1:
                data_key, data_value = next(iter(data.items()))
            else:
                raise ValueError(
                    "For map insertion, new data must be a single-key dict"
                )
            if data_key in parent:
                return
            insert_position = min(max(data_index, 0), len(keys))
            parent.insert(insert_position, data_key, data_value)
            if isinstance(data, CommentedMap):
                indent_level = max(len(route) - 1, 0)
                indent_size = getattr(self.yaml, "map_indent", 2)
                indent = indent_level * indent_size
                self.copy_comments_from_source(data, parent, data_key, indent)
        elif isinstance(parent, CommentedSeq):
            if not isinstance(anchor, tuple):
                raise ValueError(
                    f"Expected tuple (key, value) for list search, got {type(anchor)}"
                )
            anchor_key, anchor_value = anchor
            anchor_index = None
            for i, item in enumerate(parent):
                if isinstance(item, dict):
                    item_value = item.get(anchor_key)
                    if item_value == anchor_value or str(item_value) == anchor_value:
                        anchor_index = i
                        break
            if anchor_index is None:
                raise ValueError(
                    f"Item with {anchor_key}={anchor_value} not found in list"
                )
            if isinstance(data, dict):
                data_key = data.get(anchor_key) if anchor_key in data else None
                for item in parent:
                    if isinstance(item, dict):
                        item_value = item.get(anchor_key)
                        if data_key and (
                            item_value == data_key or str(item_value) == str(data_key)
                        ):
                            return
            if position == "after":
                data_index = anchor_index + 1
            else:
                data_index = anchor_index
            parent.insert(data_index, data)
        else:
            raise ValueError(f"Cannot insert sibling for parent type {type(parent)}")

    def insert_child(
        self, route: list[str], data: Any, position: ChildPosition = "end"
    ) -> None:
        if position not in get_args(ChildPosition):
            raise ValueError(
                f"Invalid position '{position}'. Must be one of"
                f" {get_args(ChildPosition)}"
            )
        parent, target, anchor = self.navigate_route(route)
        if isinstance(target, CommentedMap):
            if isinstance(data, dict) and len(data) == 1:
                data_key, data_value = next(iter(data.items()))
                target[data_key] = data_value
            else:
                raise ValueError(
                    "For map child insertion, new data must be a single-key dict"
                )
        elif isinstance(target, CommentedSeq):
            if position == "end":
                target.append(data)
            elif position == "start":
                target.insert(0, data)
            else:
                raise ValueError(f"Invalid position: {position}")
        else:
            raise ValueError(f"Cannot insert child into {type(target)}")

    def apply(self, dry_run: bool = False) -> Optional[str]:
        for spec in self.spec_data:
            path = spec.get("path", [])
            action = spec.get("action")
            data = spec.get("data")
            if action not in get_args(Action):
                raise ValueError(
                    f"Unknown action '{action}'. Must be one of {get_args(Action)}"
                )
            position = spec.get(
                "position", "after" if action == "insert_sibling" else "end"
            )
            if action == "insert_sibling":
                self.insert_sibling(path, data, position)
            elif action == "insert_child":
                self.insert_child(path, data, position)
        if dry_run:
            stream = StringIO()
            self.yaml.dump(self.data, stream)
            return stream.getvalue()
        else:
            output_path = self.output or self.path
            if output_path is None:
                raise ValueError(
                    "No input or output path specified. Provide yaml path to write"
                    " during initialization for non-dry-run."
                )
            with output_path.open("w", encoding="utf-8") as f:
                self.yaml.dump(self.data, f)
            return None
