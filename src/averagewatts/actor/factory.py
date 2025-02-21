from powerapi.pusher import PusherActor

from .actor import AverageWattsFormulaActor
from .config import AverageWattsFormulaConfig


class AverageWattsFormulaActorFactory:
    def __init__(self, config: AverageWattsFormulaConfig):
        self.config = config

    def __call__(self, name: str, pushers: dict[str, PusherActor]) -> AverageWattsFormulaActor:
        return AverageWattsFormulaActor(name, pushers, self.config)
