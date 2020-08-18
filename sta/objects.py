# ===============================================================================
# Copyright 2020 ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================
import requests
from sta.base import STBase, Related
from sta.definitions import FOOT, OM_Observation, UNITS, OTYPES, CASTS


class Location(STBase):
    api_tag = 'Locations'
    key = 'location'
    _name = None

    @property
    def name(self):
        """
        if name =='NMWDI-$autoinc' the greatest id (X) is retrieved and incremented by 1. the new name is
        NMWDI-000000<X+1>.  e.g. NMWDI-00000004

        the greatest id is only retrieved once per Location and the name is cached.
        :return:
        """
        name = self._yd['name']
        if self._name:
            name = self._name
        elif name.endswith('$autoinc'):
            # get the last NMWDI idenifier, increment 1.
            # Location.name == NMWDI-{Location.id}
            i = 0
            prefix, _ = name.split('$')
            url = "{}/Locations?$orderby=id+desc&$filter=startswith(name,  '{}')".format(self.base_url, prefix)
            resp = requests.get(url)
            if resp:
                try:
                    i = int(resp.json()['value'][0]['@iot.id'])
                except (IndexError, ValueError, TypeError) as e:
                    self.logger.warning('Failed getting latest location id: {}'.format(e))

            name = '{}{:06n}'.format(prefix, i + 1)
            self._name = name

        return name

    def payload(self):
        p = self._base_payload()
        p['encodingType'] = "application/vnd.geo+json"

        p['location'] = self.geometry
        return p


class Thing(Related):
    api_tag = 'Things'
    _location_id = None
    key = 'thing'

    def get_existing(self, url):
        url = 'Locations({})/Things'.format(self._location_id)
        return super(Thing, self).get_existing(url)

    def set_related(self, location):
        self._location_id = location.iotid
        self._related = {'Locations': [location.iotid_]}

    def payload(self):
        p = self._base_payload()
        if isinstance(self.properties, (list, dict)):
            p['properties'] = self.properties

        if self._related:
            p.update(self._related)
            return p


class Sensor(STBase):
    api_tag = 'Sensors'
    key = 'sensor'

    def payload(self):
        p = self._base_payload()
        p['encodingType'] = 'application/pdf'
        p['metadata'] = self.metadata
        return p


class ObservedProperty(STBase):
    api_tag = 'ObservedProperties'
    key = 'observed_property'

    def payload(self):
        p = self._base_payload()
        p['definition'] = self.definition
        return p


class Datastream(Related):
    api_tag = 'Datastreams'
    key = 'datastream'
    _thing_id = None

    def get_existing(self, url):
        url = 'Things({})/Datastreams'.format(self._thing_id)
        return super(Datastream, self).get_existing(url)

    @property
    def obslink(self):
        return '{}/Observations'.format(self.selflink)

    def set_related(self, thing, observedproperty, sensor):
        self._thing_id = thing.iotid
        self._related = {'Thing': thing.iotid_,
                         'ObservedProperty': observedproperty.iotid_,
                         'Sensor': sensor.iotid_}

    def payload(self):
        if self._related:
            p = self._base_payload()
            p['unitOfMeasurement'] = UNITS.get(self.unitofMeasurement.lower(), FOOT)
            p['observationType'] = OTYPES.get(self.observationType.lower(), OM_Observation)
            p.update(self._related)
            return p

    def cast(self, result):
        return CASTS.get(self.observationType.lower(), str)(result)


class Observation(Related):
    api_tag = 'Observations'

    _cast = None

    def __init__(self, yd, yl):
        super(Observation, self).__init__(yd)
        t, r = yl.split(',')
        self.phenomenonTime = t.strip()
        self.resultTime = self.phenomenonTime

        self.result = r.strip()

    def set_related(self, datastream):
        self._related = {'Datastream': datastream.iotid_}
        self._cast = datastream.cast

    def payload(self):
        if self._related:
            p = {'phenomenonTime': self.phenomenonTime,
                 'resultTime': self.resultTime,
                 'result': self._cast(self.result)}
            p.update(self._related)
            return p

    def add(self):
        super(Observation, self).add(test_unique=False)
# ============= EOF =============================================
