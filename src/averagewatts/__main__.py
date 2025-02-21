import logging
import signal
import sys
from collections import OrderedDict

from powerapi import __version__ as powerapi_version
from powerapi.backend_supervisor import BackendSupervisor
from powerapi.cli import ConfigValidator
from powerapi.cli.common_cli_parsing_manager import CommonCLIParsingManager
from powerapi.cli.generator import (
    PullerGenerator,
    PusherGenerator,
)
from powerapi.dispatch_rule import HWPCDepthLevel, HWPCDispatchRule
from powerapi.dispatcher import DispatcherActor, RouteTable
from powerapi.exception import (
    PowerAPIException,
)
from powerapi.filter import Filter
from powerapi.report import HWPCReport

from averagewatts import __version__ as naive_version
from averagewatts.actor.factory import AverageWattsFormulaActorFactory


def setup_dispatcher(config, route_table, report_filter, pushers):
    formula_factory = AverageWattsFormulaActorFactory(config)
    dispatcher = DispatcherActor(
        "naive_dispatcher", formula_factory, pushers, route_table
    )
    report_filter.filter(lambda msg: True, dispatcher)
    return dispatcher


def run_naive(config) -> None:
    logging.info(
        "Naive version %s based on PowerAPI version %s",
        naive_version,
        powerapi_version,
    )
    route_table = RouteTable()
    route_table.add_dispatch_rule(
        HWPCReport, HWPCDispatchRule(HWPCDepthLevel.SOCKET, primary=True)
    )

    report_filter = Filter()
    pullers = PullerGenerator(report_filter).generate(config)

    pushers = PusherGenerator().generate(config)

    dispatchers = {}
    dispatchers["cpu"] = setup_dispatcher(config, route_table, report_filter, pushers)

    actors = OrderedDict(**pushers, **dispatchers, **pullers)
    supervisor = BackendSupervisor(config["stream"])

    def term_handler(_, __):
        supervisor.kill_actors()
        sys.exit(0)

    signal.signal(signal.SIGTERM, term_handler)
    signal.signal(signal.SIGINT, term_handler)

    for _, actor in actors.items():
        try:
            logging.debug("Initializing actor %s...", actor.name)
            supervisor.launch_actor(actor)
        except PowerAPIException:
            logging.error("Failed to initialize actor %s", actor.name)
            supervisor.kill_actors()
            sys.exit(1)

    logging.info("Formula is now running...")
    supervisor.join()
    logging.info("Formula is shutting down...")


if __name__ == "__main__":
    args_parser = CommonCLIParsingManager()
    args = args_parser.parse()

    try:
        ConfigValidator().validate(args)
    except Exception as exn:
        logging.error("File does not exist: %s", exn)
        sys.exit(1)

    LOGGING_LEVEL = logging.DEBUG if args["verbose"] else logging.INFO
    LOGGING_FORMAT = "%(asctime)s - %(process)d - %(processName)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=LOGGING_LEVEL, format=LOGGING_FORMAT)

    run_naive(args)
    sys.exit(0)
