#!/usr/bin/env python
import requests, sqlite3

schema_field_names = ["PIN", "TAXPAYERNAME", "JURISDICTION", "PROPNAME", "PRESENTUSE", "LEVYCODE", "ADDRESS", "APPVALUE", "NUMBUILDINGS", "NUMUNITS", "LOTSQFT"]
schema_fields = ["{} text".format(name) for name in schema_field_names]
schema = "({})".format(", ".join(schema_fields))

schema_insert = ", ".join(["?" for _ in schema_field_names])

db = sqlite3.connect('results.db')
cursor = db.cursor()
cursor.execute("create table if not exists results {}".format(schema))

def save_page(pins):
    response = requests.get('https://gismaps.kingcounty.gov/parcelviewer2/pvinfoquery.ashx?pin=', params={ 'pin': pins })
    payload = response.json()
    records = [[item[name] for name in schema_field_names] for item in payload['items']]

    cursor.executemany("insert into results values ({})".format(schema_insert), records)
    db.commit()

with open('pins.csv', 'r') as pinfile:
    start_pin = cursor.execute("select max(PIN) from results").fetchone()[0]
    print("starting from last saved pin {}".format(start_pin))

    pins = []
    for pin in pinfile:
        pin = pin.rstrip()
        if start_pin != None and pin <= start_pin:
            continue
        pins.append(pin)
        if len(pins) > 9:
            save_page(pins)
            count = cursor.execute("select count(*) from results").fetchone()[0]
            print("saved new pins({}): {}".format(count, ", ".join(pins)))
            pins.clear()
