# PySTA
Python SensorThings API client


## What is PySTA
PySTA is a SensorThings API client written in python. 
It provides a simple object-based interface to SensorThings entities.  

## Who is PySTA
PySTA is developed by the New Mexico Water Data Initiative in collaboration with the Internet of Water

## Why is PySTA
PySTA was started out of the NMWDI's need for simple python tools to load data into a SensorThings instance.


## How to install
PySTA can easily be installed via [PyPI.org](https://pypi.org/project/pysta/)

```shell
pip install pysta 
```

## How to Use

Get all the locations for agency NMBGMR and output as a json file to `out.locations.json`
```
sta locations --agency NMBGMR
```

Specify an output name
```
sta locations --agency NMBGMR --out mylocations.json
```

Output as csv

```
sta locations --agency NMBGMR --out mylocations.csv
```

Output as ShapeFile
```
sta locations --agency NMBGMR --out mylocations.shp
```



Get help
```shell
sta locations --help
```
