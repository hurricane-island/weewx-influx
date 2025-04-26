"""
Influx is a platform for collecting, storing, and managing time-series data.
"""
from queue import Queue
from logging import getLogger
from distutils.version import StrictVersion
from urllib.request import urlopen
from urllib.error import HTTPError
import weewx
from weewx.restx import RESTThread, FailedPost, AbortedPost, StdRESTbase, get_site_dict
from weewx.units import convert, getStandardUnitType, unit_constants, to_std_system
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
OBS_TO_SKIP = ["dateTime", "interval", "usUnits"]
MAX_SIZE = 1000000

def _get_template(obs_key: str, overrides: dict, append_units_label: bool, unit_system):
    """get the template for an observation based on the observation key"""
    template = {}
    if append_units_label:
        unit_type = overrides.get("units")
        if unit_type is None:
            unit_type, _ = getStandardUnitType(unit_system, obs_key)
        label = UNIT_REDUCTIONS.get(unit_type, unit_type)
        if label is not None:
            template["name"] = f"{obs_key}_{label}"
    for x in ["name", "format", "units"]:
        if x in overrides:
            template[x] = overrides[x]
    return template

class Influx(StdRESTbase):
    """REST implementation for InfluxDB"""
    def __init__(self, engine, cfg_dict):
        """This service recognizes standard restful options plus the following:

        Required parameters:

        database: name of the database at the server

        Optional parameters:

        host: server hostname
        Default is localhost

        port: server port
        Default is 8086

        server_url: full restful endpoint of the server
        Default is None

        measurement: name of the measurement
        Default is 'record'

        tags: comma-delimited list of name=value pairs to identify the
        measurement.  tags cannot contain spaces.
        Default is None

        line_format: which line protocol format to use.  Possible values are
        single-line, multi-line, or multi-line-dotted.
        Default is single-line

        append_units_label: should units label be appended to name
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
        site_dict = get_site_dict(cfg_dict, "Influx", "database")
        if site_dict is None:
            return
        site_dict.setdefault("api_token", "")
        site_dict.setdefault("tags", None)
        site_dict.setdefault("line_format", "single-line")
        site_dict.setdefault("obs_to_upload", "most")
        site_dict.setdefault("append_units_label", True)
        site_dict.setdefault("augment_record", True)
        site_dict.setdefault("measurement", "record")

        log.info("database: %s", site_dict["database"])
        log.info("destination: %s", site_dict["server_url"])
        log.info("line_format: %s", site_dict["line_format"])
        log.info("measurement: %s", site_dict["measurement"])

        site_dict["append_units_label"] = to_bool(site_dict.get("append_units_label"))
        site_dict["augment_record"] = to_bool(site_dict.get("augment_record"))

        usn = site_dict.get("unit_system", None)
        if usn in unit_constants:
            site_dict["unit_system"] = unit_constants[usn]
            log.info("desired unit system: %s", usn)

        if "inputs" in cfg_dict["StdRESTful"]["Influx"]:
            site_dict["inputs"] = dict(cfg_dict["StdRESTful"]["Influx"]["inputs"])

        # if we are supposed to augment the record with data from weather
        # tables, then get the manager dict to do it.  there may be no weather
        # tables, so be prepared to fail.
        try:
            if site_dict.get("augment_record"):
                _manager_dict = weewx.manager.get_manager_dict_from_config(
                    cfg_dict, "wx_binding"
                )
                site_dict["manager_dict"] = _manager_dict
        except weewx.UnknownBinding:
            pass

        if "tags" in site_dict:
            if isinstance(site_dict["tags"], list):
                site_dict["tags"] = ",".join(site_dict["tags"])
            log.info("tags: %s", site_dict["tags"])

        # we can bind to loop packets and/or archive records
        binding = site_dict.pop("binding", "archive")
        if isinstance(binding, list):
            binding = ",".join(binding)
        log.info("binding: %s", binding)

        data_queue = Queue()
        try:
            data_thread = InfluxThread(data_queue, **site_dict)
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
        log.info("Data will be uploaded to %s", site_dict["server_url"])

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
        database: str,
        api_token: str,
        line_format="single-line",
        measurement="record",
        tags=None,
        unit_system=None,
        augment_record=True,
        inputs: dict = None,
        obs_to_upload="most",
        append_units_label=True,
        skip_upload=False,
        manager_dict=None,
        post_interval=None,
        max_backlog=MAX_SIZE,
        stale=None,
        log_success=True,
        log_failure=True,
        timeout=60,
        max_tries=3,
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
        self.database = database
        self.api_token = api_token
        self.measurement = measurement
        self.tags = tags
        self.obs_to_upload = obs_to_upload
        self.append_units_label = append_units_label
        self.inputs = inputs or {}
        self.server_url = server_url
        self.skip_upload = to_bool(skip_upload)
        self.unit_system = unit_system
        self.augment_record = augment_record
        self.line_format = line_format

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

    def format_url(self, _):
        """Format for Cloud InfluxDB 3 compatibility mode for v2 Write API"""
        return f"{self.server_url}/api/v2/write?bucket={self.database}&precision=s"

    def get_request(self, url):
        """Override and add access token"""
        request = super().get_request(url)
        request.add_header("Authorization", f"Token {self.api_token}")
        return request

    def check_response(self, response):
        if response.code == 204:
            return
        payload = response.read().decode()
        if payload and payload.find("results") >= 0:
            log.debug("code: %s payload: %s", response.code, payload)
            return
        raise FailedPost(
            f"Server returned '{payload}' ({response.code})"
        )

    def handle_exception(self, e, count):
        if isinstance(e, HTTPError):
            payload = e.read().decode()
            log.debug("exception: %s payload: %s", e, payload)
            if payload and payload.find("error") >= 0:
                if payload.find("database not found") >= 0:
                    raise AbortedPost(payload)
        super().handle_exception(e, count)

    def post_request(self, request, data=None):
        """Post the request to the server"""
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

    def get_templates(self, record: dict):
        """Parse formatting options and create a template for each observation"""
        templates = {}
        # Check every the list of variables that should be uploaded
        if self.obs_to_upload in ("all", "most"):
            for key in record.keys():
                if self.obs_to_upload == "most" and key in OBS_TO_SKIP:
                    continue
                if key not in templates:
                    templates[key] = _get_template(
                        key,
                        self.inputs.get(key, {}),
                        self.append_units_label,
                        record["usUnits"],
                    )
        else:
            for key in self.inputs.keys():
                templates[key] = _get_template(
                    key, self.inputs[key], self.append_units_label, record["usUnits"]
                )
        return templates

    @staticmethod
    def _float_to_string(template: dict[str, str], value: float, key: str, unit_system: str):
        fmt = template.get("format", "%s")
        to_units = template.get("units")
        if to_units is not None:
            from_unit, from_group = getStandardUnitType(
                unit_system, key
            )
            from_t = (value, from_unit, from_group)
            return fmt % convert(from_t, to_units)[0]
        return fmt % value

    def format_items(self, templates: dict[str, dict], tags: str, record: dict) -> list[str]:
        """
        loop through the templates, populating them with data from the
        record.
        """
        data = []
        for key, template in templates.items():
            try:
                value = record[key]
                s = InfluxThread._float_to_string(template, value, key, record["usUnits"])
                name = template.get("name", key)
                if self.line_format == "multi-line-dotted":
                    # use multiple lines with a dotted-name identifier
                    data.append(
                        f"{self.measurement}.{name}{tags} value={s} {record['dateTime']}"
                    )
                elif self.line_format == "multi-line":
                    # use multiple lines
                    data.append(
                        f"{name}{tags} value={s} {record['dateTime']}"
                    )
                else:
                    # default: use a single line
                    data.append(f"{name}={s}")
            except (TypeError, ValueError) as e:
                log.debug("skipped value '%s': %s", record.get(key), e)
        return data

    def get_post_body(self, record: dict):
        """Override my superclass and get the body of the POST"""

        # create the list of tags
        tags = ""
        binding = record.pop("binding", None)
        if binding is not None:
            tags = f",binding={binding}"
        if self.tags:
            tags = f"{tags},{self.tags}"
        templates = self.get_templates(record)

        # loop through the templates, populating them with data from the
        # record.
        data = self.format_items(templates, tags, record)
        if self.line_format in ("multi-line", "multi-line-dotted"):
            str_data = "\n".join(data)
        else:
            str_data = f"{self.measurement}{tags} {','.join(data)} {record['dateTime']}"
        return str_data, "text/plain; charset=utf-8"

