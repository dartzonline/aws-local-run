"""Route53 service emulator."""
import logging, uuid
import xml.etree.ElementTree as ET
from flask import Request, Response
from localrun.utils import error_response

logger = logging.getLogger("localrun.route53")

_NS = "https://route53.amazonaws.com/doc/2013-04-01/"
_NS_TAG = "{" + _NS + "}"


def _xml_resp(body, status=200):
    return Response(body, status, content_type="application/xml")


def _wrap(tag, inner):
    return (
        '<?xml version="1.0"?>\n'
        '<' + tag + ' xmlns="' + _NS + '">\n'
        + inner +
        '\n</' + tag + '>'
    )


def _zone_xml(z):
    return (
        "  <HostedZone>\n"
        "    <Id>" + z["Id"] + "</Id>\n"
        "    <Name>" + z["Name"] + "</Name>\n"
        "    <CallerReference>" + z["CallerReference"] + "</CallerReference>\n"
        "    <Config><Comment>" + z["Config"]["Comment"] + "</Comment>"
        "<PrivateZone>false</PrivateZone></Config>\n"
        "    <ResourceRecordSetCount>" + str(z["ResourceRecordSetCount"]) + "</ResourceRecordSetCount>\n"
        "  </HostedZone>"
    )


def _record_xml(r):
    vals = ""
    for rv in r.get("ResourceRecords", []):
        vals += "        <ResourceRecord><Value>" + rv["Value"] + "</Value></ResourceRecord>\n"
    return (
        "  <ResourceRecordSet>\n"
        "    <Name>" + r["Name"] + "</Name>\n"
        "    <Type>" + r["Type"] + "</Type>\n"
        "    <TTL>" + str(r["TTL"]) + "</TTL>\n"
        "    <ResourceRecords>\n" + vals + "    </ResourceRecords>\n"
        "  </ResourceRecordSet>"
    )


class Route53Service:
    def __init__(self):
        self.zones = {}    # zone_id -> dict
        self.records = {}  # zone_id -> list of record sets

    def handle(self, req: Request, path: str) -> Response:
        method = req.method
        # strip leading/trailing slashes
        p = path.strip("/")
        parts = p.split("/")
        # parts[0] = "2013-04-01", parts[1] = "hostedzone", ...
        if len(parts) < 2:
            return error_response("InvalidAction", "Bad path", 400)
        if len(parts) == 2 and parts[1] == "hostedzone":
            if method == "POST":
                return self._create_zone(req)
            if method == "GET":
                return self._list_zones(req)
        if len(parts) == 3 and parts[1] == "hostedzone":
            zone_id = parts[2]
            if method == "GET":
                return self._get_zone(req, zone_id)
            if method == "DELETE":
                return self._delete_zone(req, zone_id)
        if len(parts) == 4 and parts[1] == "hostedzone" and parts[3] == "rrset":
            zone_id = parts[2]
            if method == "POST":
                return self._change_records(req, zone_id)
            if method == "GET":
                return self._list_records(req, zone_id)
        return error_response("InvalidAction", f"No handler for {method} {path}", 400)

    def _create_zone(self, req):
        try:
            root = ET.fromstring(req.get_data(as_text=True))
        except ET.ParseError:
            return error_response("MalformedXML", "Bad XML body", 400)
        def _txt(tag):
            el = root.find(_NS_TAG + tag)
            if el is None:
                el = root.find(tag)
            return el.text if el is not None else ""
        name = _txt("Name")
        caller_ref = _txt("CallerReference")
        comment = ""
        cfg = root.find(_NS_TAG + "HostedZoneConfig")
        if cfg is None:
            cfg = root.find("HostedZoneConfig")
        if cfg is not None:
            cel = cfg.find(_NS_TAG + "Comment")
            if cel is None:
                cel = cfg.find("Comment")
            if cel is not None:
                comment = cel.text or ""
        if not name:
            return error_response("InvalidInput", "Name required", 400)
        if not name.endswith("."):
            name += "."
        zone_id = uuid.uuid4().hex[:14]
        z = {
            "Id": "/hostedzone/" + zone_id,
            "Name": name,
            "CallerReference": caller_ref,
            "Config": {"Comment": comment, "PrivateZone": False},
            "ResourceRecordSetCount": 2,
        }
        self.zones[zone_id] = z
        self.records[zone_id] = []
        logger.info("Route53 CreateHostedZone name=%s id=%s", name, zone_id)
        body = _wrap("CreateHostedZoneResponse", _zone_xml(z))
        return _xml_resp(body, 201)

    def _list_zones(self, req):
        inner = "  <HostedZones>\n"
        for z in self.zones.values():
            inner += _zone_xml(z) + "\n"
        inner += "  </HostedZones>\n  <IsTruncated>false</IsTruncated>\n  <MaxItems>100</MaxItems>"
        return _xml_resp(_wrap("ListHostedZonesResponse", inner))

    def _get_zone(self, req, zone_id):
        z = self.zones.get(zone_id)
        if not z:
            return error_response("NoSuchHostedZone", "Zone not found", 404)
        return _xml_resp(_wrap("GetHostedZoneResponse", _zone_xml(z)))

    def _delete_zone(self, req, zone_id):
        if zone_id not in self.zones:
            return error_response("NoSuchHostedZone", "Zone not found", 404)
        self.zones.pop(zone_id, None)
        self.records.pop(zone_id, None)
        logger.info("Route53 DeleteHostedZone id=%s", zone_id)
        body = _wrap("DeleteHostedZoneResponse", "  <ChangeInfo><Id>/change/stub</Id><Status>INSYNC</Status></ChangeInfo>")
        return _xml_resp(body)

    def _change_records(self, req, zone_id):
        if zone_id not in self.zones:
            return error_response("NoSuchHostedZone", "Zone not found", 404)
        try:
            root = ET.fromstring(req.get_data(as_text=True))
        except ET.ParseError:
            return error_response("MalformedXML", "Bad XML body", 400)
        batch = root.find(_NS_TAG + "ChangeBatch")
        if batch is None:
            batch = root.find("ChangeBatch")
        changes = []
        if batch is not None:
            chg_list = batch.find(_NS_TAG + "Changes")
            if chg_list is None:
                chg_list = batch.find("Changes")
            if chg_list is not None:
                for chg in chg_list:
                    changes.append(chg)
        for chg in changes:
            act_el = chg.find(_NS_TAG + "Action")
            if act_el is None:
                act_el = chg.find("Action")
            action = act_el.text if act_el is not None else ""
            rrs_el = chg.find(_NS_TAG + "ResourceRecordSet")
            if rrs_el is None:
                rrs_el = chg.find("ResourceRecordSet")
            if rrs_el is None:
                continue
            rec = self._parse_rrs(rrs_el)
            self._apply_change(zone_id, action, rec)
        body = _wrap(
            "ChangeResourceRecordSetsResponse",
            "  <ChangeInfo><Id>/change/stub</Id><Status>INSYNC</Status></ChangeInfo>",
        )
        return _xml_resp(body)

    def _parse_rrs(self, rrs_el):
        def _txt(tag):
            el = rrs_el.find(_NS_TAG + tag)
            if el is None:
                el = rrs_el.find(tag)
            return el.text if el is not None else ""
        name = _txt("Name")
        rtype = _txt("Type")
        ttl_raw = _txt("TTL")
        ttl = int(ttl_raw) if ttl_raw.isdigit() else 300
        vals = []
        rr_list = rrs_el.find(_NS_TAG + "ResourceRecords")
        if rr_list is None:
            rr_list = rrs_el.find("ResourceRecords")
        if rr_list is not None:
            for rr in rr_list:
                vel = rr.find(_NS_TAG + "Value")
                if vel is None:
                    vel = rr.find("Value")
                if vel is not None:
                    vals.append({"Value": vel.text or ""})
        return {"Name": name, "Type": rtype, "TTL": ttl, "ResourceRecords": vals}

    def _apply_change(self, zone_id, action, rec):
        recs = self.records[zone_id]
        name = rec["Name"]
        rtype = rec["Type"]
        existing = None
        for r in recs:
            if r["Name"] == name and r["Type"] == rtype:
                existing = r
                break
        if action in ("CREATE", "UPSERT"):
            if existing is not None:
                existing.update(rec)
            else:
                recs.append(rec)
            self.zones[zone_id]["ResourceRecordSetCount"] = 2 + len(recs)
        elif action == "DELETE":
            self.records[zone_id] = [r for r in recs if not (r["Name"] == name and r["Type"] == rtype)]
            self.zones[zone_id]["ResourceRecordSetCount"] = 2 + len(self.records[zone_id])

    def _list_records(self, req, zone_id):
        if zone_id not in self.zones:
            return error_response("NoSuchHostedZone", "Zone not found", 404)
        inner = "  <ResourceRecordSets>\n"
        for r in self.records.get(zone_id, []):
            inner += _record_xml(r) + "\n"
        inner += "  </ResourceRecordSets>\n  <IsTruncated>false</IsTruncated>\n  <MaxItems>100</MaxItems>"
        return _xml_resp(_wrap("ListResourceRecordSetsResponse", inner))

    def reset(self):
        self.zones = {}
        self.records = {}
