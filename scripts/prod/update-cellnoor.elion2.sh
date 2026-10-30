#! /usr/bin/env bash

set -euo pipefail

update-cellnoor-update
UPDATE_CELLNOOR_CONFIG_PATH=/sc/service/.config/update-cellnoor.toml UPDATE_CELLNOOR_OUTPUT_DIR=/sc/service/tmp/update-cellnoor update-cellnoor /sc/service/delivery/*/10x-genomics/25E* /sc/service/delivery/*/10x-genomics/26CH* /sc/service/delivery/*/chromium/25E* /sc/service/delivery/*/chromium/26CH*
