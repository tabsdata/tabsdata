#
# Copyright 2025 Tabs Data Inc.
#

import argparse
import os
import re
import sys

import hvac
import yaml

ENV_VAR_PATTERN = r"\${env:(\w+)(\?)?}"
HASHICORP_PATTERN = r"\${hashicorp:([^;]+;[^}]+)}"

START_TAG = "<message><i>"
END_TAG = "<message><f>"


class ConfigResolver:

    def __init__(
        self,
        hashicorp_url: str | None = None,
        hashicorp_token: str | None = None,
        hashicorp_namespace: str | None = None,
    ):
        self.strategy_to_function = {
            "env": self.resolve_env_token,
            "hashicorp": self.resolve_hashicorp_token,
        }
        self.hashicorp_config: dict[str, str | None] = {
            "url": hashicorp_url,
            "token": hashicorp_token,
            "namespace": hashicorp_namespace,
        }

    def resolve_yaml(self, path_to_yaml: str, strategies: list[str]):
        data = None
        with open(path_to_yaml, "r") as file:
            data = yaml.safe_load(file)
        for strategy in strategies:
            data = self.resolve_collection(data, strategy=strategy)
        return data

    def resolve_collection(
        self, data: dict | list | str, strategy: str
    ) -> dict | list | str:
        if isinstance(data, dict):
            for key, value in data.items():
                data[key] = self.resolve_collection(value, strategy=strategy)
        elif isinstance(data, list):
            for i, item in enumerate(data):
                data[i] = self.resolve_collection(item, strategy=strategy)
        elif isinstance(data, str):
            data = self.resolve_leaf(data, strategy=strategy)
        return data

    def resolve_leaf(self, data: str, strategy: str) -> str:
        # Resolve any kind of secret that currently exists
        try:
            data = self.strategy_to_function[strategy](data)
            return data
        except KeyError:
            raise ValueError(
                f"The strategy {strategy} is not supported. Supported "
                f"strategies are {list(self.strategy_to_function.keys())}"
            )

    def resolve_hashicorp_token(self, leaf: str) -> str:
        match = re.search(HASHICORP_PATTERN, leaf)
        while match:
            vault_url = self.hashicorp_config["url"]
            vault_token = self.hashicorp_config["token"]
            if not vault_url or not vault_token:
                raise ValueError("Hashicorp URL and token must be provided")
            vault_namespace = self.hashicorp_config["namespace"]
            hashicorp_secret_specs = match.group(1)
            path, name = hashicorp_secret_specs.split(";")
            client = hvac.Client(
                url=vault_url, token=vault_token, namespace=vault_namespace
            )
            secret = client.secrets.kv.read_secret_version(
                path, raise_on_deleted_version=False
            )
            secret_value = secret["data"]["data"][name]
            leaf = leaf.replace(match.group(0), secret_value)
            match = re.search(HASHICORP_PATTERN, leaf)
        return leaf

    @staticmethod
    def resolve_env_token(leaf: str) -> str:
        match = re.search(ENV_VAR_PATTERN, leaf)
        while match:
            env_var = match.group(1)
            is_optional = match.group(2) is not None
            env_var_value = os.getenv(env_var)
            if env_var_value is None:
                if is_optional:
                    env_var_value = ""
                else:
                    raise ValueError(f"Environment variable {env_var} not found")
            leaf = leaf.replace(match.group(0), env_var_value.strip())
            match = re.search(ENV_VAR_PATTERN, leaf)
        return leaf.strip()


def main():
    parser = argparse.ArgumentParser(
        description="Replace different secrets in a config file with their values."
    )
    parser.add_argument(
        "--input",
        type=str,
        help="Path to the file containing the configuration to resolve.",
        required=True,
    )
    parser.add_argument(
        "--strategies",
        type=str,
        help=(
            "Strategies to use when resolving the config file. If more than one is "
            "provided, they should be comma separated, and they will be resolved in "
            "the order that they are provided."
        ),
        required=True,
    )
    parser.add_argument(
        "--hashicorp-url",
        type=str,
        help="URL of the Hashicorp Vault server.",
        default=None,
    )
    parser.add_argument(
        "--hashicorp-token",
        type=str,
        help="Token to access the Hashicorp Vault server.",
        default=None,
    )
    parser.add_argument(
        "--env-hashicorp-url",
        type=str,
        help=(
            "Name of the environment variable with the URL of the Hashicorp Vault "
            "server."
        ),
        default=None,
    )
    parser.add_argument(
        "--env-hashicorp-token",
        type=str,
        help=(
            "Name of the environment variable with the token to access the Hashicorp "
            "Vault server."
        ),
        default=None,
    )
    parser.add_argument(
        "--hashicorp-namespace",
        type=str,
        help="Namespace of the Hashicorp Vault server to access.",
        default=None,
    )
    parser.add_argument(
        "--env-hashicorp-namespace",
        type=str,
        help=(
            "Name of the environment variable with the namespace to access the "
            "Hashicorp Vault server."
        ),
        default=None,
    )

    args = parser.parse_args()
    if args.env_hashicorp_url and args.hashicorp_url:
        raise ValueError(
            "Only one of hashicorp-url and env-hashicorp-url should be provided"
        )
    if args.env_hashicorp_token and args.hashicorp_token:
        raise ValueError(
            "Only one of hashicorp-token and env-hashicorp-token should be provided"
        )
    if args.env_hashicorp_namespace and args.hashicorp_namespace:
        raise ValueError(
            "Only one of hashicorp-namespace and env-hashicorp-namespace should be "
            "provided"
        )

    hashicorp_url = (
        os.getenv(args.env_hashicorp_url)
        if args.env_hashicorp_url
        else args.hashicorp_url
    )
    hashicorp_token = (
        os.getenv(args.env_hashicorp_token)
        if args.env_hashicorp_token
        else args.hashicorp_token
    )
    hashicorp_namespace = (
        os.getenv(args.env_hashicorp_namespace)
        if args.env_hashicorp_namespace
        else args.hashicorp_namespace
    )
    config_resolver = ConfigResolver(
        hashicorp_url=hashicorp_url,
        hashicorp_token=hashicorp_token,
        hashicorp_namespace=hashicorp_namespace,
    )
    resolved_data = config_resolver.resolve_yaml(args.input, args.strategies.split(","))
    sys.stdout.write(START_TAG)
    yaml.dump(resolved_data, sys.stdout)
    sys.stdout.write(END_TAG)


if __name__ == "__main__":
    main()
