"""
Influx is a platform for collecting, storing, and managing time-series data.
"""
from queue import Queue
from logging import getLogger
from typing import Union
from distutils.version import StrictVersion
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from configobj import ConfigObj
from overrides import overrides
import weewx
from weewx.restx import RESTThread, FailedPost, AbortedPost, StdRESTbase, get_site_dict
from weewx.units import convert, getStandardUnitType, unit_constants, to_std_system
from weewx.manager import get_manager_dict_from_config
from weeutil.weeutil import to_bool


REQUIRED_WEEWX = "5.1.0"
if StrictVersion(weewx.__version__) < StrictVersion(REQUIRED_WEEWX):
    raise weewx.UnsupportedFeature(
        f"weewx {REQUIRED_WEEWX} or greater is required, found {weewx.__version__}"
    )
log = getLogger(__name__)

# Shorten unit names for InfluxDB
UNIT_REDUCTIONS = {
    "degree_F": "F",
    "degree_C": "C",
    "inch": "in",
    "mile_per_hour": "mph",
    "mile_per_hour2": "mph",
    "km_per_hour": "kph",
    "km_per_hour2": "kph",
    "knot": "knot",
    "knot2": "knot",
    "meter_per_second": "mps",
    "meter_per_second2": "mps",
    "degree_compass": None,
    "watt_per_meter_squared": "Wpm2",
    "uv_index": None,
    "percent": None,
    "unix_epoch": None,
}
# observations that should be skipped when obs_to_upload is 'most'
OBS_TO_SKIP = ["dateTime", "usUnits"]
MAX_SIZE = 1000000

class LineProtocol:
    """
    Single line protocol for InfluxDB. Contains multiple fields separated by commas.
    """

    def __init__(self, measurement: str, tags: str, fields: list[str], timestamp: str):
        self.measurement = measurement
        self.tags = tags
        self.values = ",".join(fields)
        self.timestamp = timestamp

    def __str__(self):
        return f"{self.measurement}{self.tags} {self.values} {self.timestamp}"
    
    def from_record(self, measurement: str, record: dict[str], tags: str = ""):
        """Create a LineProtocol object from a record"""
        self.measurement = measurement
        self.tags = record.pop("tags", None)
        self.timestamp = record.pop("dateTime")
        self.tags = ""
        binding = record.pop("binding", None)
        if binding is not None:
            self.tags = f",binding={binding}"
        if self.tags:
            self.tags += f",{tags}"
        units = record.pop("usUnits")
        date_time = record.pop("dateTime")
        filtered = filter(self._filter_fields, record.keys())
        data = []
        for name in filtered:
            result = Observation(
                name,
                append_units=self.append_units,
                units=units,
            ).format_record(record)
            if result is not None:
                data.append(result)
        values = ",".join(data)

class Observation:
    """A template for an observation"""

    def __init__(
        self,
        name: str,
        units: str,
        append_units: bool,
        fmt: str = "%s",
    ):
        self.name = name
        self.fmt = fmt
        self.units = units
        if append_units:
            self.units, _ = getStandardUnitType(units, name)
            label = UNIT_REDUCTIONS.get(self.units, self.units)
            if label is not None:
                self.name = f"{name}_{label}"

    def convert_and_format(self, value: float, key: str, unit_system: str) -> str:
        """Unit conversion and formatting"""
        if self.units is not None:
            from_unit, from_group = getStandardUnitType(unit_system, key)
            from_t = (value, from_unit, from_group)
            return self.fmt % convert(from_t, self.units)[0]
        return self.fmt % value

    def format_record(self, record: dict) -> str:
        """
        loop through the templates, populating them with data from the
        record.
        """
        try:
            value = record[self.name]
            formatted = self.convert_and_format(value, self.name, record["usUnits"])
            return f"{self.name}={formatted}"
        except (TypeError, ValueError) as e:
            log.debug("skipped value '%s': %s", record.get(self.name), e)
            return None


class Influx(StdRESTbase):
    """REST implementation for InfluxDB"""

    def __init__(self, engine, cfg_dict):
        """This service recognizes standard restful options plus the following:

        Required parameters:

        database: name of the database at the server

        server_url: full restful endpoint of the server

        measurement: name of the measurement

        Optional parameters:

        tags: comma-delimited list of name=value pairs to identify the
        measurement.  tags cannot contain spaces.
        Default is None

        append_units: should units label be appended to name
        Default is True

        obs_to_upload: Which observations to upload.  Possible values are
        most, none, or all.  When none is specified, only items in the inputs
        list will be uploaded.  When all is specified, all observations will be
        uploaded, subject to overrides in the inputs list.
        Default is most

        inputs: dictionary of weewx observation names with optional upload
        name, format, and units
        Default is None

        binding: options include "loop", "archive", or "loop,archive"
        Default is archive
        """
        super().__init__(engine, cfg_dict)
        site_dict: ConfigObj = get_site_dict(cfg_dict, "Influx", "database")
        if site_dict is None:
            return
            # we can bind to loop packets and/or archive records
        binding = site_dict.pop("binding", "archive")
        if isinstance(binding, list):
            binding = ",".join(binding)
        append_units = to_bool(site_dict.pop("append_units"))
        select = None
        if "inputs" in cfg_dict["StdRESTful"]["Influx"]:
            select = dict(cfg_dict["StdRESTful"]["Influx"]["inputs"])

        # if we are supposed to augment the record with data from weather
        # tables, then get the manager dict to do it.  there may be no weather
        # tables, so be prepared to fail.
        augment_record = to_bool(site_dict.pop("augment_record"))
        manager_dict = None
        try:
            if augment_record:
                manager_dict = get_manager_dict_from_config(cfg_dict, "wx_binding")
        except weewx.UnknownBinding:
            pass
        data_queue = Queue()
        try:
            data_thread = InfluxThread(
                data_queue,
                manager_dict=manager_dict,
                augment_record=augment_record,
                append_units=append_units,
                select=select,
                **site_dict,
            )
        except weewx.ViolatedPrecondition as e:
            log.info("Data will not be posted: %s", e)
            return
        data_thread.start()

        if "loop" in binding.lower():
            self.loop_queue = data_queue
            self.loop_thread = data_thread
            self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)
        if "archive" in binding.lower():
            self.archive_queue = data_queue
            self.archive_thread = data_thread
            self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)
        log.info("Data will be uploaded to %s", data_thread.server_url)

    def new_loop_packet(self, event):
        """Called when a new loop packet is received"""
        data = {"binding": "loop"}
        data.update(event.packet)
        self.loop_queue.put(data)

    def new_archive_record(self, event):
        """Called when a new archive record is received"""
        data = {"binding": "archive"}
        data.update(event.record)
        self.archive_queue.put(data)


class InfluxThread(RESTThread):
    """Thread to post data to InfluxDB"""

    def __init__(
        self,
        queue: Queue,
        server_url: str,
        bucket: str,
        api_token: str,
        measurement: str,
        tags: list[str] = None,
        unit_system: str = None,
        augment_record: bool = True,
        select: dict = None,
        omit: list[str] = [],
        append_units: Union[str, bool] = True,
        # Superclass options
        manager_dict=None,
        post_interval: int = None,
        max_backlog: int = MAX_SIZE,
        stale=None,
        log_success=True,
        log_failure=True,
        max_tries=3,
        timeout=10,
        retry_wait=5,
    ):
        super().__init__(
            queue,
            protocol_name="Influx",
            manager_dict=manager_dict,
            post_interval=post_interval,
            max_backlog=max_backlog,
            stale=stale,
            log_success=log_success,
            log_failure=log_failure,
            max_tries=max_tries,
            timeout=timeout,
            retry_wait=retry_wait,
        )
        self.augment_record = to_bool(augment_record)
        if unit_system in unit_constants:
            self.unit_system = unit_constants[unit_system]
        if isinstance(tags, list):
            self.tags = ",".join(tags)
        else:
            self.tags = tags
        self.bucket = bucket
        self.api_token = api_token
        self.measurement = measurement
        self.omit = omit
        self.append_units = to_bool(append_units)
        self.select = select or {}
        self.server_url = server_url

    @overrides
    def get_record(self, record, dbmanager):
        """
        We allow the superclass to add to the record only if the user
        requests it.
        """
        if self.augment_record and dbmanager:
            record = super().get_record(record, dbmanager)
        if self.unit_system is not None:
            record = to_std_system(record, self.unit_system)
        return record

    @overrides
    def format_url(self, _):
        """Format for Cloud InfluxDB 3 compatibility mode for v2 Write API"""
        return f"{self.server_url}/api/v2/write?bucket={self.bucket}&precision=s"

    @overrides
    def get_request(self, url):
        """Override and add access token"""
        request = super().get_request(url)
        request.add_header("Authorization", f"Token {self.api_token}")
        return request

    @overrides
    def check_response(self, response):
        """Determine status of response and handle failures"""
        if response.code == 204:
            return
        payload = response.read().decode()
        if payload and payload.find("results") >= 0:
            log.debug("code: %s payload: %s", response.code, payload)
            return
        raise FailedPost(f"Server returned '{payload}' ({response.code})")

    @overrides
    def handle_exception(self, e, count):
        if isinstance(e, HTTPError):
            payload = e.read().decode()
            log.debug("exception: %s payload: %s", e, payload)
            if payload and payload.find("error") >= 0:
                if payload.find("bucket not found") >= 0:
                    raise AbortedPost(payload)
        super().handle_exception(e, count)

    @overrides
    def post_request(self, request: Request, data: str = None):
        """Supply unverified SSL context for HTTPS requests"""
        if self.server_url.startswith("https"):
            import ssl

            encoded = None
            if data:
                encoded = data.encode("utf-8")
            return urlopen(
                request,
                data=encoded,
                timeout=self.timeout,
                context=ssl._create_unverified_context(),
            )
        return super().post_request(request, data)

    def _filter_fields(self, key):
        if self.omit and key in self.omit:
            return False
        if self.select and key not in self.select:
            return False
        return True

    @overrides
    def get_post_body(self, record: dict[str]):
        """Override my superclass and get the body of the POST"""
        # create the list of tags
        tags = ""
        binding = record.pop("binding", None)
        if binding is not None:
            tags = f",binding={binding}"
        if self.tags:
            tags = f"{tags},{self.tags}"
        units = record.pop("usUnits")
        date_time = record.pop("dateTime")
        filtered = filter(self._filter_fields, record.keys())
        data = []
        for name in filtered:
            result = Observation(
                name,
                append_units=self.append_units,
                units=units,
            ).format_record(record)
            if result is not None:
                data.append(result)
        values = ",".join(data)
        str_data = f"{self.measurement}{tags} {values} {date_time}"
        return str_data, "text/plain; charset=utf-8"
