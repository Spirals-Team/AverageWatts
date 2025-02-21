# AverageWatts


To launch the formula, run the following command:
```sh
PYTHONPATH=src python -m averagewatts \
    --input csv --model HWPCReport --name puller_csv --files "../data/rapl.csv,../data/msr.csv,../data/core.csv" \
    --output csv --directory "../power_reports.d"
```
