import datetime
import os

import pytest
from influxdb_client import InfluxDBClient
from pymongo import MongoClient
from testcontainers.influxdb2 import InfluxDb2Container
from testcontainers.mongodb import MongoDbContainer

from averagewatts.__main__ import run_naive

INFLUXDB_MODE = "setup"
INFLUXDB_ADMIN_TOKEN = "DthjevVuvhlJL8xMcYNI2KLMd7D4JPy0rqLRV27fpMYCwUzfyPyTmoxeBNIYlw5vRUox-CgYXi7-fMFsictsmQ=="
INFLUXDB_USERNAME = "admin"
INFLUXDB_PASSWORD = "password"
INFLUXDB_ORG = "org-test"
INFLUXDB_BUCKET = "bucket-test"
INFLUXDB_HOST_PORT = 8086

mongo = MongoDbContainer()
influxdb = InfluxDb2Container(
    host_port=INFLUXDB_HOST_PORT,
    init_mode=INFLUXDB_MODE,
    admin_token=INFLUXDB_ADMIN_TOKEN,
    username=INFLUXDB_USERNAME,
    password=INFLUXDB_PASSWORD,
    org_name=INFLUXDB_ORG,
    bucket=INFLUXDB_BUCKET,
    retention=0,
)


@pytest.fixture(scope="module")
def setup_mongo(request):
    mongo.start()

    def teardown():
        mongo.stop()

    request.addfinalizer(teardown)

    client = MongoClient("localhost", 27017)
    return client


@pytest.fixture(scope="module")
def setup_influxdb(request):
    influxdb.start()

    def teardown():
        influxdb.stop()

    request.addfinalizer(teardown)
    url = "http://localhost:" + str(INFLUXDB_HOST_PORT)
    client = InfluxDBClient(url=url, token=INFLUXDB_ADMIN_TOKEN, org=INFLUXDB_ORG)
    return client


def _generate_hwpc_reports(number: int, timestamp: datetime.datetime):
    reports = []
    for i in range(number):
        reports.append(
            {
                "timestamp": datetime.datetime.now(),
                "sensor": "sensor-test",
                "target": f"/target{i}",
                "groups": {"core": {"0": {}}},
            }
        )
    return reports


def _generate_rapl_hwpc_reports(timestamp: datetime.datetime):
    reports = []
    for _ in range(10):
        reports.append(
            {
                "timestamp": datetime.datetime.now(),
                "sensor": "sensor-test",
                "target": "rapl",
                "groups": {"core": {"0": {}}},
            }
        )
    return reports


def _populate_mongo_db(setup_mongodb):
    db = setup_mongo["db-test"]
    coll = db["collection-test"]
    timestamp = datetime.datetime.now()
    coll.insert_one(_generate_rapl_hwpc_reports(timestamp))
    coll.insert_many(_generate_hwpc_reports(10, timestamp))


def test_csv_to_csv(tmp_path):
    tmp_dir = tmp_path / "power_reports.d"
    tmp_dir.mkdir()

    config = {
        "input": {
            "puller_csv": {
                "model": "HWPCReport",
                "files": [
                    "hwpc_reports/rapl.csv",
                    "hwpc_reports/core.csv",
                    "hwpc_reports/msr.csv",
                ],
                "type": "csv",
                "name": "puller_csv",
            }
        },
        "output": {
            "pusher_csv": {
                "directory": str(tmp_dir),
                "type": "csv",
                "model": "PowerReport",
                "name": "pusher_csv",
            }
        },
        "verbose": True,
        "stream": False,
    }
    run_naive(config)
    assert os.path.exists(tmp_dir / "k8s-master-")
    assert os.path.exists(tmp_dir / "k8s-master-rapl")
    assert len(list((tmp_dir / "k8s-master-").iterdir())) == 10
    assert len(list((tmp_dir / "k8s-master-rapl").iterdir())) == 1


def test_csv_to_influxdb(setup_influxdb):
    query_api = setup_influxdb.query_api()
    config = {
        "input": {
            "puller_csv": {
                "model": "HWPCReport",
                "files": [
                    "hwpc_reports/rapl.csv",
                    "hwpc_reports/msr.csv",
                    "hwpc_reports/core.csv",
                ],
                "type": "csv",
                "name": "puller_csv",
            }
        },
        "output": {
            "pusher_influxdb2": {
                "model": "PowerReport",
                "uri": "127.0.0.1",
                "port": INFLUXDB_HOST_PORT,
                "db": INFLUXDB_BUCKET,
                "org": INFLUXDB_ORG,
                "token": INFLUXDB_ADMIN_TOKEN,
                "type": "influxdb2",
                "name": "pusher_influxdb2",
            }
        },
        "stream": True,
        "verbose": True,
    }

    run_naive(config)
    result = query_api.query(f'from(bucket:"{INFLUXDB_BUCKET}") |> range(start: -30d)')
    results = []
    for table in result:
        for record in table.records:
            results.append((record.get_field(), record.get_value()))
    print(results)
    assert results != []


def test_mongo_connection(setup_mongo):
    _populate_mongo_db()

    assert setup_mongo.startswith("mongodb://")


def test_mock_csv_to_influxdb(setup_influxdb, mocker):
    query_api = setup_influxdb.query_api()
    config = {
        "verbose": True,
        "input": {
            "puller_csv": {
                "model": "HWPCReport",
                "files": [
                    "hwpc_reports/rapl.csv",
                    "hwpc_reports/core.csv",
                    "hwpc_reports/msr.csv",
                ],
                "type": "csv",
                "name": "puller_csv",
            }
        },
        "output": {
            "pusher_influxdb2": {
                "model": "PowerReport",
                "uri": "127.0.0.1",
                "port": INFLUXDB_HOST_PORT,
                "db": INFLUXDB_BUCKET,
                "org": INFLUXDB_ORG,
                "token": INFLUXDB_ADMIN_TOKEN,
                "type": "influxdb2",
                "name": "pusher_influxdb2",
            }
        },
        "stream": False,
    }
    run_naive(config)
    result = query_api.query(f'from(bucket:"{INFLUXDB_BUCKET}") |> range(start: -30d)')
    results = []
    for table in result:
        for record in table.records:
            results.append((record.get_field(), record.get_value()))
    print(results)
    assert results != []
