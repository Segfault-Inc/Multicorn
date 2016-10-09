"""
An implementation of OpenStack/Telemetry Foreign Data Wrapper

"""
import itertools
import json
import datetime
import dateutil.parser
import pytz

from urllib2 import Request, urlopen, URLError

from . import ForeignDataWrapper
from .utils import log_to_postgres, ERROR, WARNING


class TelemetryFdw(ForeignDataWrapper):
    """ A foreign data wrapper for accessing OpenStack Telemetry service

    Valid options:
        - username : OpenStack username for accessing the Keystone service
        - password : OpenStack password for accessing the Keystone service
        - tenant_id : OpenStack tenant id for accessing the Keystone service
        - auth_url : public Keystone service endpoint
        - meter_path : Resource path for Telemetry service  e.g. "/meters/cpu"
    """
    def __init__(self, options, columns):
        super(TelemetryFdw, self).__init__(options, columns)

        self.username = options.get('username')
        if not self.username:
            log_to_postgres('You MUST set username', ERROR)

        self.password = options.get('password')
        if not self.password:
            log_to_postgres('You MUST set password', ERROR)

        self.tenant_id = options.get('tenant_id')
        if not self.tenant_id:
            log_to_postgres('You MUST set tenant_id', ERROR)

        self.auth_url = options.get('auth_url')
        if not self.auth_url:
            log_to_postgres('You MUST set auth_url', ERROR)

        self.meter_path = options.get('meter_path')
        if not self.meter_path:
            log_to_postgres('You MUST set meter_path', ERROR)

        self.token_id = None
        self.token_expires = None
        self.metering_endpoint = None
        self.renew_period = 60
        self.columns = columns

    def execute(self, quals, columns):
        if self.token_expired():
            self.update_token()

        for metering in self.get_telemetry_response(quals):
            row = {}
            selected_columns = self.columns
            if columns:
                selected_columns = columns

            row = {}
            for column in selected_columns:
                row[column] = metering[column]

            yield row

    def get_token(self):
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}
        data = {'auth': {'passwordCredentials':
                        {'username': self.username, 'password': self.password},
                'tenantId': self.tenant_id}}
        req = Request('%s/%s' % (self.auth_url, 'tokens'),
                      json.dumps(data), headers)
        resp = urlopen(req)
        content = resp.read().decode('utf-8')
        return json.loads(content)

    def token_expired(self):
        if not self.token_id:
            return True

        now = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
        if now + datetime.timedelta(0, self.renew_period) > self.token_expires:
            return True
        return False

    def update_token(self):
        data = self.get_token()
        access = data.get('access', {})
        token = access.get('token', {})
        self.token_expires = dateutil.parser.parse(token.get('expires'))
        self.token_id = access.get('token', {}).get('id')
        service_catalog = access.get('serviceCatalog')
        metering_urls = self.urls_from_catalog(service_catalog, 'metering')
        try:
            self.metering_endpoint = metering_urls[0]
        except IndexError:
            log_to_postgres('No metering endpoint available', WARNING)

    def get_telemetry_response(self, quals):
        conditions = self.params_from_quals(quals)
        request_url = self.metering_endpoint + self.meter_path
        if conditions:
            request_url = request_url + "?" + conditions
        try:
            req = Request(request_url)
            self.upgrade_to_authenticated_request(req)
            resp = urlopen(req)
            context = resp.read().decode('utf-8')
            result = json.loads(context)
            resp.close()
        except URLError as e:
            raise Exception("Unable to connect metering service API: %s" % e)
        except Exception as e:
            raise ValueError("Unable to process metering response: %s" % e)

        return result

    def upgrade_to_authenticated_request(self, req):
        req.add_header("X-Auth-Project-Id", self.tenant_id)
        req.add_header("Accept", "application/json")
        req.add_header("X-Auth-Token", self.token_id)

    def urls_from_catalog(self, catalog, service_type):
        urls = []
        endpoints = [x.get('endpoints') for x in catalog
                     if (x.get('type') == service_type)]
        it = itertools.chain.from_iterable(endpoints)
        urls.extend(x.get('publicURL') for x in it
                    if x.get('publicURL'))
        return urls

    def params_from_quals(self, quals):
        """
        >> _params_from_quals([Qual('id', '=', 'test'), Qual('test', '>', '3')
        'q.field=id&q.op=eq&q.value=test&q.field=test&q.op=gt&q.value=3'
        """
        def map_oper(op):
            ops = {'=': 'eq', '<': 'lt', '>': 'gt'}
            return ops[op]

        return '&'.join(["q.field=%s&q.op=%s&q.value=%s" %
                         (x.field_name, map_oper(x.operator), x.value)
                         for x in quals])
