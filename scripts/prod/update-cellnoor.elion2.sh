#! /usr/bin/env bash

set -euo pipefail

# Download the binary that downloads Excel sheets
cd /sc/service/tools/bin
curl --remote-name --follow https://github.com/demhadais/update-cellnoor/releases/latest/download/download-scbl-excel-workbooks
chmod u+x download-scbl-excel-workbooks
./download-scbl-excel-workbooks --output_dir /sc/service/tmp/scbl-excel-workbooks

cd /sc/service/tmp/update-cellnoor/update-cellnoor/python

uv run main.py ../../../chromium-dataset-copies/*
