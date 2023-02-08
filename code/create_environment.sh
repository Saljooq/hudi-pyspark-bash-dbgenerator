#!/usr/bin/bash

python -m venv venv

source venv/bin/activate

pip install -r requirements.txt

echo
echo "Now you must enter 'source venv/bin/activate' to use virtual environment"
echo
echo "type 'deactivate' to leave the virtual envioronment"
echo
echo "Remember to commit all the new environment by running './update_requirement'"