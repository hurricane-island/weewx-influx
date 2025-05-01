"""
Influx is a platform for collecting, storing, and managing time-series data.

This extension for WeeWX sends weather data to InfluxDB 3 v2 using the Write API.
It is purposely limited in functionality to be simple and easy to use for specific
hardware and software configurations that we need, without many additional features.
"""

from queue import Queue
from logging import getLogger
from typing import Union, Any, Optional
from urllib.request import Request
from http.client import HTTPResponse
from configobj import ConfigObj
from overrides import overrides
from weewx import __version__, NEW_ARCHIVE_RECORD, NEW_LOOP_PACKET
from weewx.restx import RESTThread, FailedPost, StdRESTbase, get_site_dict
from influxdb_client_3 import InfluxDBClient3

log = getLogger(__name__)
REQUIRED_CONFIG = ["bucket", "server_url", "api_token", "measurement"]
ENCODING = "utf-8"
PRECISION = "s"

class ResponseMock:
    """
    Using the Influx library, we don't get a response object.
    """
    def __init__(self):
        print("Influx: Mock 204 response object created")
        self.code = 204
    def get_code(self):
        """Stub method to mimic HTTPResponse"""
        return self.code


def split_optional_csv(value: Optional[str]) -> list[str]:
    """
    Split a CSV string into a list of strings.
    """
    if value is None:
        return []
    return [item.strip() for item in value.split(",")]


# pylint: disable=too-few-public-methods
class LineProtocol:
    """
    Single line protocol for InfluxDB. Contains multiple fields separated by commas.
    Created from a database record that has been packed with additional metadata.
    Records are only used once, so we can consume them in place.
    """

    # pylint: disable=too-many-arguments, invalid-name, too-many-positional-arguments
    def __init__(
        self,
        measurement: str,
        select: list[str],
        tags: list[str],
        dateTime: Union[int, str],
        binding: Optional[str] = None,
        **kwargs,
    ):
        self.measurement = measurement
        self.timestamp = dateTime
        self.tags = f",binding={binding}" if binding is not None else ""
        if tags:
            self.tags += "," + ",".join(tags)

        def filter_fcn(item: tuple[str, float]):
            key, value = item
            return (len(select) == 0 or key in select) and value is not None

        filtered = filter(filter_fcn, kwargs.items())
        data = map(Observation.str_from_item, filtered)
        self.values = ",".join(data)

    def __str__(self):
        return f"{self.measurement}{self.tags} {self.values} {self.timestamp}"


class Observation:
    """
    A numerical observation. Records contain multiple observations.
    These are used to create the line protocol for InfluxDB.
    """

    def __init__(self, key: str, value: float):
        self.key = key
        self.value = value

    def __str__(self):
        """Key value pair for Line Protocol"""
        if self.value is None:
            return f"{self.key}=null"
        return f"{self.key}={self.value}"

    @classmethod
    def str_from_item(cls, item) -> str:
        """Create an Observation from a key, value pair"""
        key, value = item
        observation = cls(key=key, value=value)
        return str(observation)


class Influx(StdRESTbase):
    """
    REST implementation for InfluxDB. Specifically for InfluxDB 3 v2 Write API.
    This only works for cloud instances of InfluxDB. It does not work for InfluxDB 1.x or
    self-hosted InfluxDB 2.x.
    """

    def __init__(self, engine, cfg_dict: dict[str, Any]):
        """
        This service recognizes standard restful parameters plus the following
        in the configuration dictionary:

        Required:

            bucket: name of the S3 bucket at the cloud service

            server_url: full restful endpoint of the server

            measurement: name of the measurement

            api_token: token for authentication

        Optional:

            tags (Optional[str]): station tags, cannot contain whitespace.
            Default is None

            select (Optional[list[str]]): fields to select from the record.
            Default is None

            binding (str): options include "loop", "archive", or "loop,archive"
            Default is archive
        """
        super().__init__(engine, cfg_dict)
        site_dict: ConfigObj = get_site_dict(cfg_dict, "Influx", *REQUIRED_CONFIG)
        if site_dict is None:
            # No service, disabled, or missing required config
            return

        queue = Queue()
        thread = InfluxThread(
            queue=queue,
            **site_dict,
        )
        thread.start()
        if thread.loop:
            self.loop_queue = queue
            self.loop_thread = thread
            self.bind(NEW_LOOP_PACKET, self.new_loop_packet)
        if thread.archive:
            self.archive_queue = queue
            self.archive_thread = thread
            self.bind(NEW_ARCHIVE_RECORD, self.new_archive_record)

    def new_loop_packet(self, event: NEW_LOOP_PACKET) -> None:
        """Called when a new loop packet is received"""
        data = {"binding": "loop"}
        data.update(event.packet)
        self.loop_queue.put(data)

    def new_archive_record(self, event: NEW_ARCHIVE_RECORD) -> None:
        """Called when a new archive record is received"""
        data = {"binding": "archive"}
        data.update(event.record)
        self.archive_queue.put(data)


class InfluxThread(RESTThread):
    """Thread to post data to InfluxDB"""

    # pylint: disable=too-many-arguments, too-many-positional-arguments, redefined-outer-name
    @overrides
    def __init__(
        self,
        server_url: str,
        bucket: str,
        api_token: str,
        measurement: str,
        queue: Queue,
        tags: Optional[str] = None,
        select: Optional[str] = None,
        binding: str = "archive",
        **kwargs: dict[str, Any],
    ):
        super().__init__(
            queue,
            protocol_name="Influx",
            manager_dict=None,
            **kwargs,
        )
        self.bucket = bucket
        self.api_token = api_token
        self.measurement = measurement
        self.select = split_optional_csv(select)
        self.tags = split_optional_csv(tags)
        self.server_url = server_url.replace("http://", "https://")
        self.binding = binding

    @property
    def loop(self) -> bool:
        """Return True if the loop should be bound"""
        return "loop" in self.binding

    @property
    def archive(self) -> bool:
        """Return True if the archive should be bound"""
        return "archive" in self.binding

    @property
    def content_type(self) -> str:
        """Content type for the POST request"""
        return f"text/plain; charset={ENCODING}"

    @overrides
    def get_record(self, record, dbmanager) -> dict[str]:
        """Use plain record without aggregation"""
        return record

    @overrides
    def get_request(self, url: str) -> Request:
        """Add token authorization to header"""
        request = super().get_request(url)
        request.add_header("Authorization", f"Token {self.api_token}")
        return request

    @overrides
    def check_response(self, response: HTTPResponse) -> None:
        """Determine status of response and handle failures"""
        status = response.code
        if status == 204:
            return
        raise FailedPost(f"Server returned ({status})")

    @overrides
    def handle_exception(self, e, count: int) -> None:
        """Abort if bucket not found"""
        super().handle_exception(e, count)

    @overrides
    def format_url(self, _) -> str:
        """Format URL for InfluxDB Write API"""
        return f"{self.server_url}/api/v2/write?bucket={self.bucket}&precision={PRECISION}"

    @overrides
    def post_request(
        self, request: Request, data: Optional[str] = None
    ) -> HTTPResponse:
        """Make request using client API"""
        print(f"Influx: Request connection data to {self.format_url(None)}")
        with InfluxDBClient3(
            host=self.server_url,
            database=self.bucket,
            token=self.api_token
        ) as client:
            print(f"Influx: Sending data to {self.format_url(None)}")
            client.write(record=data, write_precision=PRECISION)
        return ResponseMock()

    @overrides
    def get_post_body(self, record: dict[str]) -> tuple[str, str]:
        """Format body for the POST request"""
        line = LineProtocol(
            measurement=self.measurement,
            tags=self.tags,
            select=self.select,
            **record,
        )
        formatted = str(line)
        print(f"Influx: Requesting {formatted}")
        return formatted, self.content_type

if __name__ == "__main__":
    from time import time
    from os import getenv

    test_queue = Queue()
    test_thread = InfluxThread(
        queue=test_queue,
        bucket=getenv("INFLUX_BUCKET"),
        api_token=getenv("INFLUX_API_TOKEN"),
        measurement=getenv("INFLUX_MEASUREMENT"),
        server_url=getenv("INFLUX_SERVER_URL"),
        binding="archive",
        tags="station=test",
        select="outTemp,inTemp,outHumidity",
        max_tries=1,
    )

    record = {
        "dateTime": int(time()),
        "outTemp": 33.5,
        "inTemp": 75.8,
        "outHumidity": 24,
    }

    test_queue.put(record)
    test_queue.put(None)
    test_thread.run()
