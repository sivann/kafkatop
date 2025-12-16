## Python Version (Legacy)

# Building

Requires python >=3.9 in your path


1. set the full path of PYTHON at the top of Makefile or add the PYTHON= parameter when calling make
2. ```make```
3. ```make pex```


```
make pex
```
This will create a "kafkatop" pex executable which will include the python code and library dependencies all in one file. It will need the python3.9-python3.12 in the path to run.


