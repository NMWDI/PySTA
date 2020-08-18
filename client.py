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
import logging

from sta.objects import Location, Thing, Sensor, ObservedProperty, Datastream, Observation


class Client(object):
    def __init__(self):
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        self.logger = logging.getLogger('st')
        self.logger.setLevel(logging.DEBUG)

    def upload_to_st(self, yd):
        location = Location(yd)
        location.add()
        self.logger.debug('Added location')

        thing = Thing(yd)
        thing.set_related(location)
        thing.add()
        self.logger.debug('Added thing')

        sensor = Sensor(yd)
        sensor.add()
        self.logger.debug('Added sensor')

        obprop = ObservedProperty(yd)
        obprop.add()
        self.logger.debug('Added observed property')

        ds = Datastream(yd)
        ds.set_related(thing, obprop, sensor)
        ds.add()
        self.logger.debug('Added datastream')

        for oi in yd['observations']:
            obs = Observation(yd, oi)
            obs.set_related(ds)
            obs.add()

        self.logger.debug('added observations')

        metadata = {'sensorthings': {'location_link': location.selflink,
                                     'thing_link': thing.selflink,
                                     'sensor_link': sensor.selflink,
                                     'observed_property_link': obprop.selflink,
                                     'datastream_link': ds.selflink,
                                     'observations_link': ds.obslink}}
        return metadata
# ============= EOF =============================================
