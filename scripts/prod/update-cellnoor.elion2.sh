#! /usr/bin/env bash

set -euo pipefail

/sc/service/tools/bin/download-scbl-excel-workbooks --config_path /sc/service/.config/update-cellnoor/download-scbl-excel-workbooks.toml --output_dir /sc/service/tmp/update-cellnoor/scbl-excel-workbooks

cd /sc/service/tmp/update-cellnoor/update-cellnoor/python/update-cellnoor
module unload conda && module load /sc/service/tools/modules/languages/uv
uv run main.py --config-path /sc/service/.config/update-cellnoor/update-cellnoor.toml /sc/service/delivery/*/10x-genomics/25E* /sc/service/delivery/*/chromium/26CH*
