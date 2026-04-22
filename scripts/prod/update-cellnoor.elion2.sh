#! /usr/bin/env bash

# Download the binary that downloads Excel sheets
cd /sc/service/tools/bin
curl --remote-name --follow https://github.com/demhadais/update-cellnoor/releases/latest/download/download-scbl-excel-workbooks
chmod u+x download-scbl-excel-workbooks
./download-scbl-excel-workbooks --config_path /sc/service/.config/update-cellnoor/download-scbl-excel-workbooks.toml --output_dir /sc/service/tmp/scbl-excel-workbooks

# We assume this directory exists because we are running this script after `copy-chromium-datasets.elion2.sh` which clones the update-cellnoor repo
cd /sc/service/tmp/update-cellnoor/update-cellnoor/python

uv run main.py ../../../chromium-dataset-copies/*
