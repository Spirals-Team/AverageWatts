import re

from powerapi.formula import FormulaState


class AverageWattsFormulaState(FormulaState):
    def __init__(self, actor, pushers, metadata, config):
        super().__init__(actor, pushers, metadata)
        self.config = config

        m = re.search(r"^\(\'(.*)\', \'(.*)\', \'(.*)\'\)$", actor.name)
        self.dispatcher = m.group(1)
        self.sensor = m.group(2)
        self.socket = m.group(3)

    def __repr__(self):
        return f"AverageWattsFormulaState(dispatcher={self.dispatcher},sensor={self.sensor},socket={self.socket},config={self.config},metadata={self.metadata})"
