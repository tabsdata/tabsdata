#
# Copyright 2026 Tabsdata Inc.
#

from dataclasses import dataclass

FAMILIES = ("assets", "configs", "pods")

YAML_EXTENSIONS = (".yaml", ".yml")

SCHEMA_MARKER = "schema"
SCHEMA_SUFFIX = ".yaml"
SCHEMA_SOURCE_EXTENSION = ".schema.yaml"

PROVIDER_COLORS = {
    "kminus": "#8B5A2B",
    "docker": "#2EAD68",
    "eks": "#FF9900",
    "fargate": "#8C4FFF",
    "aks": "#0078D4",
    "gke": "#EA4335",
}

GENERIC_COLOR = "white"


@dataclass(frozen=True)
class ParsedName:
    name: str
    tokens: tuple[str, ...]
    extensions: str

    @property
    def stripped(self) -> str:
        return f"{self.name}{self.extensions}"

    @property
    def rendered(self) -> str:
        tokens = "".join(f"_{token}" for token in self.tokens)
        return f"{self.name}{tokens}{self.extensions}"


def parse_name(filename: str) -> ParsedName:
    dot = filename.find(".")
    head, extensions = (filename[:dot], filename[dot:]) if dot >= 0 else (filename, "")

    if "_" in head:
        segments = head.split("_")
        return ParsedName(
            name=segments[0],
            tokens=tuple(segments[1:]),
            extensions=extensions,
        )

    parts = filename.split(".")
    for position, part in enumerate(parts):
        if "_" not in part:
            continue
        segments = part.split("_")
        trailing = parts[position + 1 :]
        return ParsedName(
            name=".".join([*parts[:position], segments[0]]),
            tokens=tuple(segments[1:]),
            extensions=f".{'.'.join(trailing)}" if trailing else "",
        )

    return ParsedName(name=head, tokens=(), extensions=extensions)


def is_yaml(filename: str) -> bool:
    return filename.endswith(YAML_EXTENSIONS)


def is_schema(filename: str) -> bool:
    if filename.endswith(SCHEMA_SOURCE_EXTENSION):
        return True
    parsed = parse_name(filename)
    if parsed.extensions != SCHEMA_SUFFIX:
        return False
    return parsed.name.split(".")[-1] == SCHEMA_MARKER


def parsable(family: str, filename: str) -> bool:
    return family != "assets" and is_yaml(filename)


def provider_of(filename: str) -> str | None:
    tokens = parse_name(filename).tokens
    return tokens[-1] if tokens else None


def color_of(filename: str) -> str:
    provider = provider_of(filename)
    if provider is None:
        return GENERIC_COLOR
    return PROVIDER_COLORS.get(provider, GENERIC_COLOR)


def colored(filename: str) -> str:
    return f"[{color_of(filename)}]{filename}[/]"
