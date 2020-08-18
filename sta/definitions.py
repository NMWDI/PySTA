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

# this is a duplication of the cv definitions service.
# eventually this extractor should pull this info from the cv service
# instead of having it stored locally here

FOOT = {'name': 'Foot',
        'symbol': 'ft',
        'definition': 'http://www.qudt.org/vocab/unit/FT'}
DEGC = {'name': 'Degree Celsius',
        'symbol': 'degC',
        'definition': 'http://www.qudt.org/vocab/unit/DEG_C'}

GAL = {'name': 'Gallon',
       'symbol': 'gal',
       'definition': 'http://qudt.org/vocab/unit/GAL_US'}

PPM = {'name': 'Parts Per Million',
       'symbol': 'PPM',
       'definition': 'http://www.qudt.org/vocab/unit/PPM'}

OM_CategoryObservation = 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_CategoryObservation'
OM_CountObservation = 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_CountObservation'
OM_Measurement = 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement'
OM_Observation = 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Observation'
OM_TruthObservation = 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_TruthObservation'

OTYPES = {'double': OM_Measurement,
          'uri': OM_CategoryObservation,
          'integer': OM_CountObservation,
          'any': OM_Observation,
          'boolean': OM_TruthObservation}

CASTS = {'double': float,
         'uri': str,
         'integer': int,
         'any': str,
         'boolean': bool}

UNITS = {'foot': FOOT,
         'feet': FOOT,
         'c': DEGC,
         'ppm': PPM,
         'gallon': GAL,
         'gal': GAL}
# ============= EOF =============================================
