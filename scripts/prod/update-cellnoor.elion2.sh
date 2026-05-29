#! /usr/bin/env bash

set -euo pipefail

/sc/service/tools/bin/download-scbl-excel-workbooks --output_dir /sc/service/tmp/update-cellnoor/scbl-excel-workbooks
cd /sc/service/tmp/update-cellnoor/update-cellnoor/python/update-cellnoor

module unload conda && module load /sc/service/tools/modules/languages/uv
uv run main.py ../../../chromium-dataset-copies/*
