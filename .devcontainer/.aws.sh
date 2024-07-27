#!/bin/sh
#
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

if [ -d ~/.aws ]; then
  if echo "$(mountpoint ~/.aws)" | grep -q "is a mountpoint"; then
    # ~/.aws is a bind mount from the host
    return 0;
  fi
  echo "$(/bin/ls -a /mnt/aws 2>/dev/null)" > /tmp/ls_mnt_aws
  echo "$(/bin/ls -a ~/.aws 2>/dev/null)" > /tmp/ls_aws
  echo "$(/bin/ls -a /tmp/.aws 2>/dev/null)" > /tmp/ls_tmp_aws
  if [ -d /mnt/aws ] && [ -z "$(comm -3 /tmp/ls_mnt_aws /tmp/ls_aws)" ]; then
    # /mnt/aws and ~/.aws are the same in terms of file names.
    rm /tmp/ls_mnt_aws
    rm /tmp/ls_aws
    rm /tmp/ls_tmp_aws
    return 0;
  fi
  if [ -d /tmp/.aws ] && [ -z "$(comm -3 /tmp/ls_tmp_aws /tmp/ls_aws)" ]; then
    # Retro-compatibility: /tmp/.aws and ~/.aws are the same in terms of file names.
    rm /tmp/ls_mnt_aws
    rm /tmp/ls_aws
    rm /tmp/ls_tmp_aws
    return 0;
  fi
  rm /tmp/ls_mnt_aws
  rm /tmp/ls_aws
  rm /tmp/ls_tmp_aws
fi

if [ -d /tmp/.aws ]; then
  # Retro-compatibility
  echo "Copying content of /tmp/.aws to ~/.aws"
  mkdir -p ~/.aws
  cp -r /tmp/.aws/* ~/.aws/
  chmod 600 ~/.aws/*
  chmod 644 ~/.aws/*.pub &> /dev/null
  return 0
fi
if [ ! -d /mnt/aws ]; then
  echo "No bind mounted aws directory found (~/.aws, /tmp/.aws, /mnt/aws), exiting"
  return 0
fi

if [ "$(stat -c '%U' /mnt/aws)" != "UNKNOWN" ]; then
  echo "Unix host detected, symlinking /mnt/aws to ~/.aws"
  rm -rf ~/.aws
  ln -s /mnt/aws ~/.aws
  return 0
fi

echo "Windows host detected, copying content of /mnt/aws to ~/.aws"
mkdir -p ~/.aws
cp -rf /mnt/aws/* ~/.aws/
chmod 600 ~/.aws/*
