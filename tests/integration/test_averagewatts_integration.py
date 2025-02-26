import datetime
import os

import pytest
from bson.json_util import loads
from influxdb_client import InfluxDBClient
from pymongo import InsertOne
from testcontainers.influxdb2 import InfluxDb2Container
from testcontainers.mongodb import MongoDbContainer

from averagewatts.__main__ import run_naive

INFLUXDB_MODE = "setup"
INFLUXDB_RETENTION = 0
INFLUXDB_ADMIN_TOKEN = "DthjevVuvhlJL8xMcYNI2KLMd7D4JPy0rqLRV27fpMYCwUzfyPyTmoxeBNIYlw5vRUox-CgYXi7-fMFsictsmQ=="
INFLUXDB_USERNAME = "admin"
INFLUXDB_PASSWORD = "password"
INFLUXDB_ORG = "org-test"
INFLUXDB_BUCKET = "bucket-test"
INFLUXDB_URI = "127.0.0.1"
INFLUXDB_HOST_PORT = 8086
INFLUXDB_TYPE = "influxdb2"

MONGODB_DB = "test"
MONGODB_COLLECTION = "sensor"
MONGODB_TYPE = "mongodb"

CSV_TYPE = "csv"
CSV_RAPL_FILE_PATH = "test/integration/data/rapl.csv"
CSV_MSR_FILE_PATH = "test/integration/data/msr.csv"
CSV_CORE_FILE_PATH = "test/integration/data/core.csv"

INPUT_REPORT_MODEL = "HWPCReport"
OUTPUT_REPORT_MODEL = "PowerReport"

mongo = MongoDbContainer("mongo:7.0.7")
influxdb = InfluxDb2Container(
    host_port=INFLUXDB_HOST_PORT,
    init_mode=INFLUXDB_MODE,
    admin_token=INFLUXDB_ADMIN_TOKEN,
    username=INFLUXDB_USERNAME,
    password=INFLUXDB_PASSWORD,
    org_name=INFLUXDB_ORG,
    bucket=INFLUXDB_BUCKET,
    retention=INFLUXDB_RETENTION,
)


# Probleme semble venir du container mongo, en attente. Les datas ne sont pas inséré dans la db.
@pytest.fixture(scope="module")
def setup_mongo(request):
    print("Starting MongoDB...")
    mongo.start()
    client = mongo.get_connection_client()

    def teardown():
        mongo.stop()

    request.addfinalizer(teardown)

    client = mongo.get_connection_client()
    _populate_mongodb(client)
    return mongo.get_connection_url()


@pytest.fixture(scope="module")
def setup_influxdb(request):
    influxdb.start()

    def teardown():
        influxdb.stop()

    request.addfinalizer(teardown)

    url = "http://localhost:" + str(INFLUXDB_HOST_PORT)
    client = InfluxDBClient(url=url, token=INFLUXDB_ADMIN_TOKEN, org=INFLUXDB_ORG)
    return client


def _generate_core_hwpc_reports(number: int, timestamp: datetime.datetime):
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


def _populate_mongodb(client):
    db = client.test
    collection = db.sensor
    requesting = []
    with open("tests/integration/data/mongodb.json") as file:
        for jsonObj in file:
            myDict = loads(jsonObj)
            requesting.append(InsertOne(myDict))

    collection.bulk_write(requesting)

    client.close()


def test_csv_to_csv(tmp_path):
    tmp_dir = tmp_path / "power_reports.d"
    tmp_dir.mkdir()

    config = {
        "input": {
            "puller_csv": {
                "model": INPUT_REPORT_MODEL,
                "files": [
                    CSV_CORE_FILE_PATH,
                    CSV_MSR_FILE_PATH,
                    CSV_RAPL_FILE_PATH,
                ],
                "type": CSV_TYPE,
                "name": "puller_csv",
            }
        },
        "output": {
            "pusher_csv": {
                "directory": str(tmp_dir),
                "type": CSV_TYPE,
                "model": OUTPUT_REPORT_MODEL,
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
                "model": INPUT_REPORT_MODEL,
                "files": [
                    CSV_CORE_FILE_PATH,
                    CSV_MSR_FILE_PATH,
                    CSV_RAPL_FILE_PATH,
                ],
                "type": CSV_TYPE,
                "name": "puller_csv",
            }
        },
        "output": {
            "pusher_influxdb2": {
                "model": OUTPUT_REPORT_MODEL,
                "uri": INFLUXDB_URI,
                "port": INFLUXDB_HOST_PORT,
                "db": INFLUXDB_BUCKET,
                "org": INFLUXDB_ORG,
                "token": INFLUXDB_ADMIN_TOKEN,
                "type": INFLUXDB_TYPE,
                "name": "pusher_influxdb2",
            }
        },
        "stream": False,
        "verbose": True,
    }

    run_naive(config)

    result = query_api.query(f'from(bucket:"{INFLUXDB_BUCKET}") |> range(start: -30d)')
    results = []
    for table in result:
        for record in table.records:
            results.append((record.get_field(), record.get_value()))

    assert results != []


def test_mongo_to_influxdb(setup_influxdb, setup_mongo):
    query_api = setup_influxdb.query_api()
    config = {
        "input": {
            "puller_mongodb": {
                "model": INPUT_REPORT_MODEL,
                "uri": setup_mongo,
                "db": MONGODB_DB,
                "collection": MONGODB_COLLECTION,
                "type": MONGODB_TYPE,
                "name": "puller_mongodb",
            }
        },
        "output": {
            "pusher_influxdb2": {
                "model": OUTPUT_REPORT_MODEL,
                "uri": INFLUXDB_URI,
                "port": INFLUXDB_HOST_PORT,
                "db": INFLUXDB_BUCKET,
                "org": INFLUXDB_ORG,
                "token": INFLUXDB_ADMIN_TOKEN,
                "type": INFLUXDB_TYPE,
                "name": "pusher_influxdb2",
            }
        },
        "stream": False,
        "verbose": False,
    }

    run_naive(config)

    result = query_api.query(f'from(bucket:"{INFLUXDB_BUCKET}") |> range(start: -30d)')
    results = []
    for table in result:
        for record in table.records:
            results.append((record.get_field(), record.get_value()))

    assert results != []


def test_mongo_to_csv(tmp_path, setup_mongo):
    tmp_dir = tmp_path / "power_reports.d"
    tmp_dir.mkdir()

    config = {
        "input": {
            "puller_mongodb": {
                "model": INPUT_REPORT_MODEL,
                "uri": setup_mongo,
                "db": MONGODB_DB,
                "collection": MONGODB_COLLECTION,
                "type": MONGODB_TYPE,
                "name": "puller_mongodb",
            }
        },
        "output": {
            "pusher_csv": {
                "directory": str(tmp_dir),
                "type": CSV_TYPE,
                "model": OUTPUT_REPORT_MODEL,
                "name": "pusher_csv",
            }
        },
        "verbose": False,
        "stream": False,
    }

    run_naive(config)

    assert os.path.exists(tmp_dir / "ssw01-")
    assert os.path.exists(tmp_dir / "ssw01-rapl")
    assert len(list((tmp_dir / "ssw01-").iterdir())) == 11
    assert len(list((tmp_dir / "ssw01-rapl").iterdir())) == 1
