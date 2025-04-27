# Influx Cloud Extension

WeeWx extension that sends data to an influx data server

(C) 2016-2024 Matthew Wall
(C) 2025      Hurricane Island Center for Science and Leadership

## Installation

This plugin is intended to be downloaded with git, and embedded in a Docker container,
for distribution to a device fleet with Balena Cloud. This involves templating the 
configuration file and updating it from the device-level environment variables.

```docker
RUN git clone https://github.com/hurricane-island/weewx-influx.git /root/weewx-influx
COPY ./weewx.template.conf ./template.py /root/weewx-data/
```

This mimics the WeeWx installation process, so there is no longer an `install.py` file, 
since the container should be discarded and rebuilt if something changes.

## Configuration

Configuration requires a `server_url`, `api_token`, `bucket`, and `measurement`.
The bucket must already exist before a connection can be established, and the
API token must have write permissions in that bucket.

You can also optionally specify global `tags`, a `binding` to either or both of loop and archive events,
variables to `select` from each record to upload.

For example:

[StdRESTful]
    [[Influx]]
        bucket = weather
        server_url = https://us-east-1-1.aws.cloud2.influxdata.com
        api_token = $API_TOKEN
        measurement = weather_stations
        binding = archive
        tags = Rockland, Vantage
        select = outTemp, inTemp, outHumidity
