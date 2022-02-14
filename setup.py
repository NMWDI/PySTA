# ===============================================================================
# Copyright 2021 ross
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

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="pysta",
    version="0.0.26",
    author="Jake Ross",
    description="Python SensorThings Client",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/NMWDI/pysta",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "Click",
        "paho-mqtt",
        "pyproj",
        "requests",
        "jsonschema",
        "pyodata",
        "pyyaml",
        "pyshp",
        "shapely",
    ],
    entry_points={
        "console_scripts": [
            "sta = sta.cli:cli",
        ],
    },
    python_requires=">=3.6",
    # include_package_data=True,
    packages=["sta"],
    # package_data={
    #     # If any package contains *.txt or *.rst files, include them:
    #     "templates": ["*.template",],
    # }
)
# ============= EOF =============================================
