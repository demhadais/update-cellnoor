#! /usr/bin/env bash

# Download the binary that downloads Excel sheets
cd /sc/service/tools/bin
curl --remote-name --follow https://github.com/demhadais/update-cellnoor/releases/download/download-scbl-excel-workbooks-latest/download-scbl-excel-workbooks
chmod u+x download-scbl-excel-workbooks
./download-scbl-excel-workbooks --config_path /sc/service/.config/update-cellnoor/download-scbl-excel-workbooks.toml --output_dir /sc/service/tmp/scbl-excel-workbooks

# Download the python package that sends these updates to the cellnoor API
cd /sc/service/tmp
git clone https://github.com/demhadais/update-cellnoor
cd update-cellnoor/python
uv run main.py --config-path /sc/service/.config/update-cellnoor/update-cellnoor.toml /sc/service/delivery/*/*/2*
