#!/usr/bin/bash

# to prevent any confusion from python3
sudo apt install python-is-python3

# now we make sure venv command would work
python -m pip install virtualenv

# next we make sure the cache is emptied so we might not be importing
# any broken packages and downloading and installing from scratch
rm -rf ~/.cache/pip/*

echo "now feel free to start the editor if you didn't receive any errors"


