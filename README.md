# AverageWatts

AverageWatts is a formula for a software-defined power meter based on the [`PowerAPI framework`](https://github.com/powerapi-ng/powerapi).
This project is the implementation of a simple power meter that estimate the power consumption following this method :
$\mathcal{P} = \frac{Number_of_processus}{\mathcal{P} rapl}$

## Installation

Formula can be launched by running the following command:
```sh
PYTHONPATH=src python -m averagewatts \
    --input <Input> \
    --output <Output>
```
Input / Output are the one available from [`PowerAPI framework`](https://github.com/powerapi-ng/powerapi).

For example, using csv as a source and csv as a destination:
```sh
PYTHONPATH=src python -m averagewatts \
    --input csv --model HWPCReport --name puller_csv --files "../tests/integration/data/rapl.csv,../tests/integration/data/msr.csv,../tests/integrations/data/core.csv" \
    --output csv --directory "../power_reports.d"
```
