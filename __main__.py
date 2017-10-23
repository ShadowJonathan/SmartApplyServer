import SocketServer
from BaseHTTPServer import BaseHTTPRequestHandler
import urlparse
import yaml
import os.path
from pydal import *
import json
import re

if not os.path.isfile("config.yaml"):
    print "config.yaml does not exist, please fill in the default_config.yaml file with the right information, " \
          "and rename it to \"config.yaml\""
    exit(1)

with open("config.yaml", 'r') as stream:
    Y = yaml.load(stream)
    IP = Y.get("server")["ip"]
    PORT = Y.get("server")["port"]
    DB_URL = Y.get("database")["url"]

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


def process_query(self):
    self.query = urlparse.parse_qs(urlparse.urlparse(self.path).query)


def uqlf(thing):
    thing = s(thing)
    match = re.search(r'^"(.*?)"$', thing)
    if match:
        thing = match.group(1)
    return lf(thing)


def lf(thing):
    return "%" + s(thing) + "%"


def i(thing):
    return int(s(thing))


def s(thing):
    if isinstance(thing, list):
        thing = thing[0]
    return thing


def find(query):
    import datetime

    def handler(x):
        if isinstance(x, datetime.datetime):
            return x.isoformat()
        raise TypeError("Unknown type")

    S = db.applications.id > 0
    if "match" in query:
        S &= db.applications.name.like(uqlf(query['match'])) | \
             db.applications.description.like(uqlf(query['match'])) | \
             db.applications.application.like(uqlf(query['match']))

    if "want_above" in query:
        S &= db.applications.want > i(query["want_above"])
    elif "want_below" in query:
        S &= db.applications.want < i(query["want_above"])

    if "cat" in query:
        for cat in query['cat'][0].split("|"):
            S &= db.applications.categories.like(lf("|" + cat + "|"))

    print S

    obj = []
    for row in db(S).select():
        cats = []
        for cat in row.categories:
            cats.append(cat.name)
        obj.append(
            {"id": row.id, "name": row.name, "website": row.website, "categories": cats, "update": row.update_date,
             "description": row.description, "application": row.application, "want": row.want,
             "status": row.status, "country": row.country})
    return json.dumps(obj, default=handler)


class ApplyHandler(BaseHTTPRequestHandler):
    query = None

    def do_GET(self):
        process_query(self)
        if "get_categories" in self.query:
            self.send_response(201)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            obj = []
            for row in db().select(db.categories.ALL):
                obj.append({"id": row.id, "name": row.name})

            self.wfile.write(json.dumps(obj))
        elif "search" in self.query:
            try:
                resp = find(self.query)
                self.send_response(201)
                self.send_header('Content-type', 'application/json')
                self.end_headers()

                self.wfile.write(resp)
            except:
                self.send_error(500)
        else:
            self.send_response(404)


httpd = SocketServer.TCPServer((IP, PORT), ApplyHandler)
httpd.serve_forever()
