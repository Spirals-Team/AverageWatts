import datetime
import logging
import math
from collections import OrderedDict
from typing import Any

from powerapi.handler import Handler
from powerapi.report import HWPCReport, PowerReport


class HWPCReportHandler(Handler):
    """
    HwPC reports handler.
    """

    def __init__(self, state):
        super().__init__(state)
        self.ticks: OrderedDict[datetime.datetime, dict[str, HWPCReport]] = (
            OrderedDict()
        )

    def handle(self, msg: HWPCReport) -> None:
        """
        Process a HWPC report and send the result(s) to a pusher actor.
        :param msg: Received HWPC report
        """
        logging.debug("received message: %s", msg)

        current_tick = self.ticks.setdefault(msg.timestamp, {})
        if msg.target in current_tick:
            logging.warning(
                "Duplicate HWPCReport for target %s at timestamp %s. ",
                msg.target,
                msg.timestamp,
            )

        current_tick[msg.target] = msg
        # Start to process the oldest tick only after receiving at least 5 ticks.
        # We wait before processing the ticks in order to mitigate the possible delay between the sensor/database.
        if len(self.ticks) >= 5:
            power_reports = self._process_oldest_tick()
            for report in power_reports:
                for name, pusher in self.state.pushers.items():
                    pusher.send_data(report)
                    logging.debug("sent report: %s to %s", report, name)

    def _process_oldest_tick(self) -> list[PowerReport]:
        """
        Process the oldest tick stored in the stack and generate power reports for the running target(s).
        :return: Power reports of the running target(s)
        """
        timestamp, hwpc_reports = self.ticks.popitem(last=False)

        global_report: HWPCReport = hwpc_reports.pop("all", None)
        if not global_report:
            logging.error("Failed to process tick %s: missing global report", timestamp)
            return []

        if not hwpc_reports:
            # Pre-processor can drop reports
            logging.error("No available reports !")
            return []

        logging.debug("processing tick %s", timestamp)
        logging.debug("global report: %s", hwpc_reports)

        # Retrieve rapl energy measurement
        energy = next(
            iter(global_report.groups["rapl"][str(self.state.socket)].values())
        )
        # Convert Joules to Watts
        energy_in_watts = math.ldexp(energy["RAPL_ENERGY_PKG"], -32)

        event_count = len(hwpc_reports)
        power_estimation = energy_in_watts / event_count

        # per-target power estimation
        power_reports: list[PowerReport] = [
            self._gen_power_report(
                timestamp,
                target_name,
                "naive",
                power_estimation,
                1.0,
                target_report.metadata,
            )
            for target_name, target_report in hwpc_reports.items()
        ]

        # rapl power
        power_reports.append(
            self._gen_power_report(
                timestamp,
                "rapl",
                "naive",
                energy_in_watts,
                1.0,
                global_report.metadata,
            )
        )
        return power_reports

    def _gen_power_report(
        self,
        timestamp: datetime,
        target: str,
        formula: str,
        power: float,
        ratio: float,
        metadata: dict[str, Any],
    ) -> PowerReport:
        """
        Generate a power report using the given parameters.
        :param timestamp: Timestamp of the measurements
        :param target: Target name
        :param formula: Formula identifier
        :param power: Power estimation
        :return: Power report filled with the given parameters
        """
        report_metadata = metadata | {
            "scope": "cpu",
            "socket": self.state.socket,
            "formula": formula,
            "ratio": ratio,
        }
        return PowerReport(timestamp, self.state.sensor, target, power, report_metadata)
