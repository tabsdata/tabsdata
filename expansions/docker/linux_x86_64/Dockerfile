#
# Copyright 2025 Tabs Data Inc.
#

FROM ghcr.io/cross-rs/x86_64-unknown-linux-musl:main

RUN export DEBIAN_FRONTEND=noninteractive
RUN export TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update
RUN apt-get install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa -y
RUN apt-get update

RUN apt-get install -y curl

RUN apt-get install -y python3.12
RUN apt-get install -y python3.12-dev
RUN apt-get install -y python3.12-venv

RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.12 1

RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.12
RUN python -m pip install --upgrade pip

RUN python --version
RUN python3 --version
RUN pip --version
RUN pip3 --version

ENV RUST_VERSION=1.88.0
ENV MATURIN_VERSION=1.9.3

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain ${RUST_VERSION}
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup target add x86_64-unknown-linux-musl

RUN pip install maturin[patchelf]==${MATURIN_VERSION}

RUN rustc --version
RUN cargo --version
RUN maturin --version
RUN patchelf --version

RUN rm -rf /var/lib/apt/lists/*
