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
import csv
import json

import click

from sta.client import Client


@click.group()
def cli():
    pass


@cli.command()
@click.option("--name")
@click.option("--agency")
@click.option("--verbose/--no-verbose", default=False)
@click.option("--out")
def things(name, agency, verbose, out):
    client = Client()

    query = []
    if name:
        query.append(f"name eq '{name}'")

    if agency:
        query.append(f"Locations/properties/agency eq '{agency}'")

    query = " and ".join(query)
    records = list(client.get_things(query if query else None))
    if verbose:
        for li in records:
            click.secho(li)

    cnt = len(records)
    click.secho(f"Found {cnt} Things")
    output(out, records, query, client.base_url)


@cli.command()
@click.option("--name")
@click.option("--agency")
@click.option("--verbose", default=False)
@click.option("--out")
def locations(name, agency, verbose, out):
    client = Client()

    query = []
    if name:
        query.append(f"name eq '{name}'")

    if agency:
        query.append(f"properties/agency eq '{agency}'")

    query = " and ".join(query)
    records = list(client.get_locations(query))
    if verbose:
        for li in records:
            click.secho(li)

    cnt = len(records)
    click.secho(f"Found {cnt} Locations")
    output(out, records, query, client.base_url)


def output(out, records, query, base_url):
    if out:
        with open(out, "w") as wfile:
            if out.endswith(".csv"):
                writer = csv.writer(wfile)
                count = 0
                for emp in records:
                    if count == 0:
                        # Writing headers of CSV file
                        header = emp.keys()
                        writer.writerow(header)
                        count += 1

                    # Writing data of CSV file
                    writer.writerow(emp.values())
            else:
                data = {"data": records, "query": query, "base_url": base_url}
                json.dump(data, wfile, indent=2)

            click.secho(f"wrote nrecords={len(records)} to {out}")


if __name__ == "__main__":
    things()
# ============= EOF =============================================
