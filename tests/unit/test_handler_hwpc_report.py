import datetime
import random
from collections import OrderedDict

import pytest
from powerapi.report import HWPCReport

from naive.handler import HWPCReportHandler

NUMBER_OF_GENERATED_CORE_REPORTS = 10

RAPL_ENERGY_PKG = 11757944832
RAPL_POWER_IN_WATTS = 2.73760986328125
ESTIMATED_POWER = RAPL_POWER_IN_WATTS / NUMBER_OF_GENERATED_CORE_REPORTS


def _generate_hwpc_reports(all=True, core=True) -> list[HWPCReport]:
    reports = {}
    timestamp = datetime.datetime.now()
    rapl_groups = {
        "rapl": {
            "0": {
                "11": {
                    "RAPL_ENERGY_PKG": RAPL_ENERGY_PKG,
                }
            }
        }
    }
    if all:
        reports["all"] = HWPCReport(
            timestamp,
            "test-sensor",
            "all",
            rapl_groups,
        )
    if core:
        for _ in range(NUMBER_OF_GENERATED_CORE_REPORTS):
            sensor = "test-sensor"
            target = "/system.test/" + str(random.randrange(1000))
            groups = {"core": {}}
            reports[target] = HWPCReport(timestamp, sensor, target, groups)

    return OrderedDict([(timestamp, reports)])


@pytest.fixture
def mock_hwpc_handler(mocker):
    mock_state = mocker.MagicMock()
    mock_state.socket = "0"
    mock_state.sensor = "test_sensor"
    return HWPCReportHandler(mock_state)


@pytest.fixture
def processed_reports_setup(mocker, mock_hwpc_handler):
    handler = mock_hwpc_handler

    hwpc_reports = _generate_hwpc_reports()
    handler.ticks = hwpc_reports

    return handler


def test_returns_correct_number_of_reports(processed_reports_setup):
    processed_reports = processed_reports_setup._process_oldest_tick()
    assert NUMBER_OF_GENERATED_CORE_REPORTS + 1 == len(processed_reports)


def test_rapl_power_convertion_to_watts_is_correct(processed_reports_setup):
    processed_reports = processed_reports_setup._process_oldest_tick()
    rapl_power_report = processed_reports[-1]
    assert RAPL_POWER_IN_WATTS == rapl_power_report.power


def test_power_estimation_is_correct(processed_reports_setup):
    processed_reports = processed_reports_setup._process_oldest_tick()
    core_power_reports = processed_reports[:-1]
    for power_report in core_power_reports:
        assert ESTIMATED_POWER == power_report.power


@pytest.fixture
def processed_reports_without_core_reports(mock_hwpc_handler):
    handler = mock_hwpc_handler

    hwpc_reports = _generate_hwpc_reports(core=False)
    handler.ticks = hwpc_reports

    return handler


def test_missing_core_reports(processed_reports_without_core_reports, caplog):
    processed_reports = processed_reports_without_core_reports._process_oldest_tick()
    assert 0 == len(processed_reports)

    for record in caplog.records:
        assert record.levelname == "ERROR"
        assert "No available reports !" in record.message


@pytest.fixture
def processed_reports_without_all_reports(mock_hwpc_handler):
    handler = mock_hwpc_handler

    hwpc_reports = _generate_hwpc_reports(all=False)
    handler.ticks = hwpc_reports

    return handler


def test_missing_all_reports(processed_reports_without_all_reports, caplog):
    processed_reports = processed_reports_without_all_reports._process_oldest_tick()

    assert 0 == len(processed_reports)

    for record in caplog.records:
        assert record.levelname == "ERROR"
        assert "Failed to process tick" in record.message
