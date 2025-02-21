# AverageWatts


To launch the formula, navigate to the src directory:
```sh
cd src
```

And then run the following command:
```sh
python -m averagewatts \
    --input csv --model HWPCReport --name puller_csv --files "../data/rapl.csv,../data/msr.csv,../data/core.csv" \
    --output csv --directory "../power_reports.d"
```
