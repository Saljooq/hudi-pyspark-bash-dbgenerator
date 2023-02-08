# Code

This part contains
- a handful of bashscripts
- requirements.txt to load a local environment (with create_environment.txt)
- main package in src

To run the main script simpy type 
```bash
$ ./code/create_environment.sh
$ source venv/bin/activate
$ cd code/src/hudi_pysp*
$ python3 main.py 
```

The proper way to open your project for development is to run `./start_editor.sh` from the outer folder