# GottaGoFAASt Final Project

## Requirements

To install dependencies establish a virtual environment and then run the following command:

```
pip install -r requirements.txt
```

## Files

`main.py` contains our REST API, `task_dispatcher.py` is the Task Dispatcher for the FAAS, `pull_worker.py` and `push_worker.py` are the remote worker instances that can be spun up to help the task dispatcher complete tasks.

## Performance

Find our performance worker in `perf/` where we keep the bash script `test_perf.sh` which allows for testing on the different worker types

### How to Run Perf

```
sh perf/test_perf.sh <worker_mode> <dispatcher_url>
```

## Reports

Find our reports in `reports/` where we have `technical_report.md`, `performance_report.md`, and `testing_report.md`!