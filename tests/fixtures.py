"""Fixture API responses covering every quirk found while exploring the real API."""
from datetime import datetime, timedelta, timezone

U = "http://environment.data.gov.uk/flood-monitoring"
# The notebooks' clock is frozen here (see local_databricks.FrozenDatetime): late enough in the day that every fixture
# reading for "today" is complete, and always in the past, so results never depend on when the tests run.
NOW = (datetime.now(timezone.utc) - timedelta(days=1)).replace(hour=23, minute=59, second=0, microsecond=0)
D1 = (NOW - timedelta(days=2)).date().isoformat()
D2 = (NOW - timedelta(days=1)).date().isoformat()


def ISO(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def station(ref, uri=None, **kw):
    s = {"@id": f"{U}/id/stations/{uri or ref}", "stationReference": ref, "notation": ref, "label": f"Station {ref}",
         "type": [f"{U}/def/core/Station", f"{U}/def/core/SingleLevel"], "measures": [{"@id": "m"}]}
    s.update(kw)
    return s

STATIONS = [
    station("1029TH", status=f"{U}/def/core/statusActive", lat=51.87, long=-1.74, easting=417600, northing=220300,
            riverName="Dikler", catchmentName="Cotswolds", town="Little Rissington", dateOpened="1994-01-01"),
    station("E1234", status=[f"{U}/def/core/statusSuspended"], riverName="Avon", newApiField="surprise"),  # list status, no coords, drift
    station("2001", status=f"{U}/def/core/statusukcmf", lat=52.1, long=-1.2),                                  # unknown status
    station("1029TH", uri="1029TH-dup", lat=51.87, long=-1.74),                                                 # duplicate reference
    {"@id": f"{U}/id/stations/NOREF", "label": "No reference station"},                                        # quarantine
]

M1 = "1029TH-level-downstage-i-15_min-mASD"
M2 = "E1234-flow--i-15_min-m3_s"
M3 = "2001-level-stage-i-15_min-mASD"   # silent: nothing in the daily files, backlog appears via catch-up
MEASURES = [
    {"@id": f"{U}/id/measures/{M1}", "notation": M1, "label": "Dikler level", "station": f"{U}/id/stations/1029TH",
     "stationReference": "1029TH", "parameter": "level", "parameterName": "Water Level", "qualifier": "Downstream Stage",
     "period": 900, "unit": "http://qudt.org/1.1/vocab/unit#Meter", "unitName": "mASD", "valueType": "instantaneous",
     "latestReading": {"dateTime": ISO(NOW - timedelta(hours=20)), "value": -0.35}},
    # no notation and no stationReference: both must be derived from the URIs
    {"@id": f"{U}/id/measures/{M2}", "label": "Avon flow", "station": f"{U}/id/stations/E1234",
     "parameter": "flow", "period": 900, "unitName": "m3/s", "latestReading": {"dateTime": ISO(NOW - timedelta(hours=30)), "value": 3.2}},
    {"@id": f"{U}/id/measures/{M3}", "notation": M3, "station": f"{U}/id/stations/2001", "stationReference": "2001",
     "parameter": "level", "period": 900, "unitName": "mASD", "latestReading": {"dateTime": ISO(NOW - timedelta(hours=2)), "value": 1.1}},
    {"label": "measure with no id or notation"},                                                               # quarantine
]

def rd(m, dt, value="__none__"):
    r = {"@id": f"{U}/data/readings/{m}/{dt}", "dateTime": dt, "measure": f"{U}/id/measures/{m}"}
    if value != "__none__":
        r["value"] = value
    return r

def day_readings(day, correction=False):
    out = []
    for h in range(0, 24, 6):
        out.append(rd(M1, f"{day}T{h:02d}:00:00Z", -0.30 - h / 100))
        out.append(rd(M2, f"{day}T{h:02d}:00:00Z", 3.0 + h / 10))
    out += [
        rd(M1, f"{day}T01:00:00Z", "0.121|0.122"),       # pipe-joined pair
        rd(M2, f"{day}T01:00:00Z", [0.5, 0.6]),          # list value
        rd(M1, f"{day}T02:00:00Z"),                       # NaN -> value omitted
        rd(M2, f"{day}T02:00:00Z", "abc"),               # non-numeric
        rd(M1, "not-a-date", 1.0),                        # invalid timestamp
    ]
    if correction:                                       # a later run corrects one value and adds a late reading
        out[0]["value"] = -9.99
        out.append(rd(M1, f"{day}T23:00:00Z", -0.40))
    return out

def catchup_readings():
    return [rd(M3, ISO(NOW - timedelta(days=10, hours=h)), 1.0 + h / 100) for h in range(0, 48, 12)]
