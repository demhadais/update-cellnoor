#! /usr/bin/env bash

set -euo pipefail

cd /sc/service/tmp/update-cellnoor
cd update-cellnoor/python/update-cellnoor

uv run copy_chromium_datasets.py ../../../chromium-dataset-copies
