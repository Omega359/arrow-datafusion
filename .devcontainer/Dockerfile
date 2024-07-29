# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Dockerfile based on https://github.com/qdm12/rustdevcontainer/tree/main/.devcontainer
ARG BASEDEV_VERSION=v0.25.0

FROM qmcgaw/basedevcontainer:${BASEDEV_VERSION}-debian
ARG CREATED
ARG COMMIT
ARG VERSION=local
LABEL \
    org.opencontainers.image.authors="quentin.mcgaw@gmail.com" \
    org.opencontainers.image.created=$CREATED \
    org.opencontainers.image.version=$VERSION \
    org.opencontainers.image.revision=$COMMIT \
    org.opencontainers.image.url="https://github.com/qdm12/rustdevcontainer" \
    org.opencontainers.image.documentation="https://github.com/qdm12/rustdevcontainer" \
    org.opencontainers.image.source="https://github.com/qdm12/rustdevcontainer" \
    org.opencontainers.image.title="Rust Debian Dev container" \
    org.opencontainers.image.description="Rust development container"
WORKDIR /apache_datafusion

# Install Rust for the correct CPU architecture
ARG RUST_VERSION=1.80.0
ARG RUSTUP_INIT_VERSION=1.27.1
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
RUN arch="$(uname -m)" && \
    case "$arch" in \
    x86_64) rustArch='x86_64-unknown-linux-gnu'; rustupSha256='6aeece6993e902708983b209d04c0d1dbb14ebb405ddb87def578d41f920f56d' ;; \
    aarch64) rustArch='aarch64-unknown-linux-gnu'; rustupSha256='1cffbf51e63e634c746f741de50649bbbcbd9dbe1de363c9ecef64e278dba2b2' ;; \
    *) echo >&2 "unsupported architecture: ${dpkgArch}"; exit 1 ;; \
    esac && \
    wget -qO /tmp/rustup-init "https://static.rust-lang.org/rustup/archive/${RUSTUP_INIT_VERSION}/${rustArch}/rustup-init" && \
    echo "${rustupSha256}  /tmp/rustup-init" | sha256sum -c - && \
    chmod +x /tmp/rustup-init && \
    /tmp/rustup-init -y --no-modify-path --profile minimal --default-toolchain ${RUST_VERSION} --default-host ${rustArch} && \
    rm /tmp/rustup-init && \
    chmod -R a+w ${RUSTUP_HOME} ${CARGO_HOME}

# Install:
# - unzip libxext6 libxext-dev libxtst-dev libxi-dev freetype* procps gdb - required by intellij (https://www.jetbrains.com/help/idea/prerequisites-for-dev-containers.html)
# - build-essential, libc6-dev, clang required by Rust
# - musl-tools required for static binaries
# - sccache used to speed up cargo builds
# - mold used to speed up linking
# - vim because, well, vi
# - protobuf-compiler for proto file compilation
# - pkg-config, libssl-dev required for build
# - npm - required to run prettier
RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get update && \
    apt-get -y upgrade && \
    apt-get -y install unzip libxext6 libxext-dev libxtst-dev libxi-dev freetype* procps gdb build-essential clang libc6-dev musl-tools sccache mold vim protobuf-compiler pkg-config libssl-dev npm && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install Rust tooling
ARG RUST_ANALYZER_VERSION=2024-07-01
RUN wget -qO- "https://github.com/rust-analyzer/rust-analyzer/releases/download/${RUST_ANALYZER_VERSION}/rust-analyzer-$(uname -m)-unknown-linux-gnu.gz" | \
    gunzip > /usr/local/bin/rust-analyzer && \
    chmod 500 /usr/local/bin/rust-analyzer
RUN rustup component add clippy rustfmt
RUN rustup component add llvm-tools-preview

# Shell setup
COPY .zshrc-specific .welcome.sh .ssh.sh /root/
RUN mkdir ~/.zfunc && rustup completions zsh > ~/.zfunc/_rustup && mkdir -p ~/.ssh && chmod 600 ~/.ssh


