# Big brother

Set of pythons scripts to generate http metrics, pass them over kafka and save to postgresql database.
It's possible to change configuration of metrics generator via `config.py`

## Requirements

- python3.9
- kafka
- postgresql

## Installation

```
pip install -r .meta/packages_dev
```

## Run

Update PYTHONPATH
```
export PYTHONPATH=.
```

Run producer
```
python jobs/producer_job.py
```

Run consumer
```
python jobs/consumer_job.py
```

## Tests

```
pytest
```
