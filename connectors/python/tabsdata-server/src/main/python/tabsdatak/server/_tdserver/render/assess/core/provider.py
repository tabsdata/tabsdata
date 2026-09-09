#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from enum import Enum


class Provider(Enum):
    EKS = "eks"
    AKS = "aks"
    GKE = "gke"
    KMINUS = "kminus"

    @property
    def caption(self) -> str:
        return CAPTIONS[self]

    @property
    def hosted(self) -> bool:
        return self is not Provider.KMINUS

    @property
    def checks_host_ports(self) -> bool:
        return self is Provider.KMINUS

    @property
    def sign_in(self) -> tuple[str, ...]:
        return SIGN_IN[self]

    @property
    def auth_plugin(self) -> str | None:
        return AUTH_PLUGIN[self]


CAPTIONS: dict[Provider, str] = {
    Provider.EKS: "AWS EKS",
    Provider.AKS: "Azure AKS",
    Provider.GKE: "Google Cloud GKE",
    Provider.KMINUS: "KMinus",
}

SIGN_IN: dict[Provider, tuple[str, ...]] = {
    Provider.EKS: (
        "aws sso login",
        "aws eks update-kubeconfig --name <CLUSTER> --region <REGION>",
    ),
    Provider.AKS: (
        "az login --tenant <TENANT>",
        "az aks get-credentials --resource-group <GROUP> --name <CLUSTER>",
        "kubelogin convert-kubeconfig -l azurecli",
    ),
    Provider.GKE: (
        "gcloud auth login",
        "gcloud container clusters get-credentials <CLUSTER>"
        " --region <REGION> --project <PROJECT>",
    ),
    Provider.KMINUS: (),
}

AUTH_PLUGIN: dict[Provider, str | None] = {
    Provider.EKS: None,
    Provider.AKS: "kubelogin",
    Provider.GKE: "gke-gcloud-auth-plugin",
    Provider.KMINUS: None,
}


def provider_of(name: str | None) -> Provider | None:
    if not name:
        return None
    try:
        return Provider(name.lower())
    except ValueError:
        return None
