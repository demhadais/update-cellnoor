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

# The only complexity here is that the API token is associated with a user, not a "service". We're expecting that user to supply their API token through environment variables
uv run main.py --config-path /sc/service/.config/update-cellnoor/update-cellnoor.toml --api-token $CELLNOOR_API_TOKEN /sc/service/delivery/*/*/2*
