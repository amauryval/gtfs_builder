GTFS BUILDER

[![RunTest](https://github.com/amauryval/gtfs_builder/actions/workflows/main.yml/badge.svg?branch=master)](https://github.com/amauryval/gtfs_builder/actions/workflows/main.yml)
[![codecov](https://codecov.io/gh/amauryval/gtfs_builder/branch/master/graph/badge.svg)](https://codecov.io/gh/amauryval/gtfs_builder)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)


A tool to compute all the intermediate stops (geometry and time range) between each trips from a GTFS in order to emulate their travel 


## Install the environment with pyenv and poetry

```bash
pyenv local 3.10.7
poetry env use 3.10.7
poetry install
```


then activate the conda env

```bash
conda activate gtfs_builder
```

## Configure parameters files

### params.json

Each block are used to compute all the moving stops from the specified GTFS:

1. set the name of your gtfs without space ("study_area_name")
2. Copy paste your data into the 'input_data' (for example) directory ("input_data_dir")
3. Select the transport modes to compute in a list ("transport_modes")
4. Select the date filter mode: based on "calendar_dates.txt" or "calendar.text" ("date_mode")
5. Define the date to compute ("date")
6. Set if you want to build the "shapes.txt" file ("build_shape_id")
7. Set the interpolation nodes values: 100 = 1 nodes interpolated each 100 meters ("interpolation_threshold")
8. Set if you want to proceed data with multiprocessing mode. Good only if you have a lot of trips! ("multiprocess")
```
[
    {
        "study_area_name": "your_area_name",
        "input_data_dir": "input_data",
        "transport_modes": ["train"],
        "date_mode": "calendar_dates",
        "date": "20211125",
        "build_shape_id": true,
        "interpolation_threshold": 3500,
        "multiprocess": false
    },
    {
        "study_area_name": "your_area_name_2",
        "input_data_dir": "input_data_2",
        "transport_modes": ["tramway", "metro"],
        "date_mode": "calendar",
        "date": "20191115",
        "build_shape_id": false,
        "interpolation_threshold": 1000,
        "multiprocess": true
    }
]
```

### inputs_attrs.json

Copy paste the 'inputs_attrs.json' file into your data direction (ex: 'input_data')
This file is used to map column types for each gtfs files


## Run the process

Let's go to compute your data

```
python db_run.py
```

You'll get 3 parquet files:

* [study_area_name]_moving_stops.parq:

Contains all the stops interpolated

* [study_area_name]_base_stops_data.parq

On going.... [TODO]

* [study_area_name]_base_lines_data.parq

On going.... [TODO]



