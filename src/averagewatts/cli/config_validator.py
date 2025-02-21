from powerapi.cli import ConfigValidator


class AverageWattsConfigValidator(ConfigValidator):
    @staticmethod
    def validate(config: dict):
        ConfigValidator.validate(config)
