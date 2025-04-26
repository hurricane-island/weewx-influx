from weecfg.extension import ExtensionInstaller

def loader():
    """Load the InfluxDB installer."""
    return InfluxInstaller()

class InfluxInstaller(ExtensionInstaller):
    """Installer implementation for InfluxDB."""
    def __init__(self):
        super(InfluxInstaller, self).__init__(
            version="0.17",
            name='influx',
            description='Upload weather data to Influx Cloud.',
            author="Matthew Wall",
            author_email="mwall@users.sourceforge.net",
            restful_services='user.influx.Influx',
            config={
                'StdRESTful': {
                    'Influx': {
                        'database': 'INSERT_DATABASE_HERE',
                        'server_url': 'INSERT_HOST_HERE',
                        'api_token': 'INSERT_API_TOKEN_HERE'
                    }}},
            files=[('bin/user', ['bin/user/influx.py'])]
            )
