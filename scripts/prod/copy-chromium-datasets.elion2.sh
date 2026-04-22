#! /usr/bin/env bash

cd /sc/service/tmp/update-cellnoor
rm -rf update-cellnoor
git clone https://github.com/demhadais/update-cellnoor
cd update-cellnoor/python/update-cellnoor

uv run copy_chromium_datasets.py ../../../chromium-dataset-copies
