import logging

from powerapi.formula import FormulaActor
from powerapi.handler import PoisonPillMessageHandler, StartHandler
from powerapi.message import PoisonPillMessage, StartMessage
from powerapi.pusher import PusherActor
from powerapi.report import HWPCReport

from averagewatts.handler import HWPCReportHandler

from .config import AverageWattsFormulaConfig
from .state import AverageWattsFormulaState


class AverageWattsFormulaActor(FormulaActor):
    def __init__(
        self,
        name,
        pushers: dict[str, PusherActor],
        config: AverageWattsFormulaConfig,
        level_logger=logging.WARNING,
        timeout=None,
    ):
        super().__init__(name, pushers, level_logger, timeout)
        self.state = AverageWattsFormulaState(self, pushers, self.formula_metadata, config)

    def setup(self):
        super().setup()
        self.add_handler(StartMessage, StartHandler(self.state))
        self.add_handler(PoisonPillMessage, PoisonPillMessageHandler(self.state))
        self.add_handler(HWPCReport, HWPCReportHandler(self.state))
