import json
import yaml
import os.path
from pydal import *
import sys
from datetime import datetime
import dateutil.parser

if not os.path.isfile("config.yaml"):
    print "cannot feed data without a configured config.yaml"
    exit(1)

with open("config.yaml", 'r') as stream:
    Y = yaml.load(stream)
    DB_URL = Y.get("database")["url"]


def make_datetime(json_date):
    if json_date is None:
        return None
    try:
        return datetime.strptime(json_date, '%Y-%m-%dT%H:%M:%S.%fZ')
    except:
        pass
    try:
        return datetime.strptime(json_date, '%Y-%m-%dT%H:%M:%S.%f')
    except:
        pass
    try:
        return datetime.strptime(json_date, '%Y-%m-%dT%H:%M:%S')
    except:
        pass
    return dateutil.parser.parse(json_date)


### DEFINE TABLE BEGIN
db = DAL(DB_URL)
db.define_table(
    "categories",
    Field("name", "string", required=True, unique=True)
)
db.define_table(
    "applications",
    Field("name", "string", required=True),
    Field("website", "string"),
    Field("update_date", "datetime"),
    Field("description", "text"),
    Field("application", "text"),
    Field("want", "integer"),
    Field("status", "string"),
    Field("categories", "list:reference categories"),
    Field("country", "string")
)
### DEFINE TABLE END

with open(sys.argv[1], 'r') as data:
    for entry in json.load(data):
        categories = []
        for cat in entry['categories']:
            db.categories.update_or_insert(name=cat)
            categories.append(db(db.categories.name == cat).select()[0].id)
        db.commit()

        print categories

        db.applications.insert(name=entry['name'], website=entry['website'], description=entry['description'],
                               update_date=make_datetime(entry["update"]), application=entry['application'],
                               want=(entry['want'] or 0),
                               status=entry['status'], categories=categories, country=entry['country'])

db.commit()
