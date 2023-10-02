#!/usr/bin/env python3

# Copyright © Aptos Foundation
# SPDX-License-Identifier: Apache-2.0

# Test replay-verify by running it on a public testnet backup
# While the replay-verify composite Github Action is meant to run with aptos-core checked out in the current
# working directory, this test script is meant to be run from this separate repo. The environment variable APTOS_CORE_PATH
# is required to be set to the path of your local checkout of aptos-core, which will be used to build and copy over test dependencies.

import os
import subprocess

import replay_verify


def local_setup():
    # Take these from the expected replay verify run
    envs = {
        "TIMEOUT_MINUTES": "5",
        "BUCKET": "aptos-testnet-backup-b7b1ad7a",
        "SUB_DIR": "e1",
        "HISTORY_START": "350000000",
        "TXNS_TO_SKIP": "46874937 151020059",
        "BACKUP_CONFIG_TEMPLATE_PATH": "terraform/helm/fullnode/files/backup/gcs.yaml",
    }

    # build backup tools
    subprocess.run(
        [
            "cargo",
            "build",
            "--release",
            "-p",
            "aptos-db-tool",
        ],
        check=True,
    )

    # write to environment variables
    for key, value in envs.items():
        os.environ[key] = value


if __name__ == "__main__":
    local_setup()
    replay_verify.main(1, 1)