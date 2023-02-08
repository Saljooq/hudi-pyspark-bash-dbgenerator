#!/usr/bin/bash

# First we go to the code directory
cd code/

# Next we create a virtual environment with the correct python version
# and the proper dependencies
./create_environment.sh

# Next we activate the virtual environment
source venv/bin/activate


# Next we install the package in editable mode for linting support
python -m pip install -e .

# Next we launch the editor from this instance to get proper linting
code ..
