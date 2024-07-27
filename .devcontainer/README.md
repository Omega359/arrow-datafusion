<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Datafusion development container

This development container can be used with RustRover or IntelliJ to create and run a docker image for development
purposes. It is based on this [Rust dev container](https://github.com/qdm12/rustdevcontainer/tree/main/.devcontainer)
and it should work on Linux, Windows and OSX though currently on OSX IntelliJ required Docker and it will not work with
podman.

## Requirements

- [Docker](https://www.docker.com/products/docker-desktop) installed and running
- [Docker Compose](https://docs.docker.com/compose/install/) installed

## Setup prior to running the dev container:

1. Create the following files on your host if you don't have them:

    ```shell
    touch ~/.gitconfig ~/.zsh_history ~/.aws
    ```
1. Configure git ssh url rewrite in ~/.gitconfig on your host by adding the following:
   ```shell
   [url "git@github.com:"]
     insteadOf = https://github.com/
   ```
   Note that the development container will create the empty directories `~/.docker`, `~/.ssh` if you don't have them.

1. **For Docker on OSX or Windows without WSL**: ensure your home directory `~` is accessible by Docker.

1. In RustRover/IntelliJ -> File -> Remote Development... -> Dev Containers -> New Dev Container -> Select From VCS
   Project and enter the git url for this repository in the `git repository:` field. VSCode should also work but is
   untested.
1. RustRover/IntelliJ will proceed to build the container and run it. Note that the docker-compose.yml mounts the ~/.ssh
   and ~/.aws into the container so that git and aws commands will function correctly (assuming they work on your local
   machine).

## Customization

### Customize the image

You can make changes to the [Dockerfile](Dockerfile) and then rebuild the image.

### Publish a port

To access a port from your host to your development container, publish a port
in [docker-compose.yml](docker-compose.yml). If you use vscode you can also now do it directly with VSCode without
restarting the container.

### Run other services

1. Modify [docker-compose.yml](docker-compose.yml) to launch other services at the same time as this development
   container, such as a test database:

    ```yml
      database:
        image: postgres
        restart: always
        environment:
          POSTGRES_PASSWORD: password
    ```

1. In [devcontainer.json](devcontainer.json), change the line `"runServices": ["datafusion"],`
   to `"runServices": ["datafusion", "database"],`.
1. Rebuild the container.
