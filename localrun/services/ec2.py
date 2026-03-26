"""EC2 stub service emulator (query-string Action= protocol)."""
import logging, time, uuid
from urllib.parse import parse_qs
from flask import Request, Response
from localrun.config import get_config
from localrun.utils import error_response, new_request_id

logger = logging.getLogger("localrun.ec2")

_NS = "http://ec2.amazonaws.com/doc/2016-11-15/"
_REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
_INSTANCE_TYPES = ["t2.micro", "t2.small", "t3.micro", "t3.small", "m5.large", "m5.xlarge", "c5.large"]

def _rid():
    return new_request_id()

def _iid():
    return "i-" + uuid.uuid4().hex[:17]

def _vid():
    return "vpc-" + uuid.uuid4().hex[:17]

def _sid():
    return "subnet-" + uuid.uuid4().hex[:17]

def _sgid():
    return "sg-" + uuid.uuid4().hex[:17]

def _volid():
    return "vol-" + uuid.uuid4().hex[:17]

def _ami_id():
    return "ami-" + uuid.uuid4().hex[:17]

class EC2Service:
    def __init__(self):
        self.instances = {}        # instance_id -> dict
        self.vpcs = {}             # vpc_id -> dict
        self.subnets = {}          # subnet_id -> dict
        self.security_groups = {}  # sg_id -> dict
        self.key_pairs = {}        # name -> dict
        self.images = {}           # image_id -> dict
        self.volumes = {}          # volume_id -> dict
        self._init_defaults()

    def _init_defaults(self):
        c = get_config()
        vpc_id = _vid()
        self.vpcs[vpc_id] = {
            "VpcId": vpc_id, "CidrBlock": "172.31.0.0/16",
            "IsDefault": True, "State": "available",
            "OwnerId": c.account_id,
        }
        sn_id = _sid()
        self.subnets[sn_id] = {
            "SubnetId": sn_id, "VpcId": vpc_id,
            "CidrBlock": "172.31.0.0/20",
            "AvailabilityZone": c.region + "a",
            "DefaultForAz": True, "State": "available",
            "OwnerId": c.account_id,
        }
        sg_id = _sgid()
        self.security_groups[sg_id] = {
            "GroupId": sg_id, "GroupName": "default",
            "Description": "default VPC security group",
            "VpcId": vpc_id, "OwnerId": c.account_id,
            "IpPermissions": [], "IpPermissionsEgress": [],
        }

    def _handlers(self):
        return {
            "DescribeInstances": self._desc_instances,
            "RunInstances": self._run_instances,
            "TerminateInstances": self._terminate,
            "StartInstances": self._start_instances,
            "StopInstances": self._stop_instances,
            "DescribeInstanceStatus": self._instance_status,
            "DescribeVpcs": self._desc_vpcs,
            "DescribeSubnets": self._desc_subnets,
            "DescribeSecurityGroups": self._desc_sgs,
            "CreateSecurityGroup": self._create_sg,
            "DeleteSecurityGroup": self._delete_sg,
            "AuthorizeSecurityGroupIngress": self._auth_sg_ingress,
            "AuthorizeSecurityGroupEgress": self._auth_sg_egress,
            "DescribeKeyPairs": self._desc_keypairs,
            "CreateKeyPair": self._create_keypair,
            "DeleteKeyPair": self._delete_keypair,
            "DescribeImages": self._desc_images,
            "DescribeRegions": self._desc_regions,
            "DescribeAvailabilityZones": self._desc_azs,
            "DescribeVolumes": self._desc_volumes,
            "CreateVolume": self._create_volume,
            "DeleteVolume": self._delete_volume,
            "DescribeInstanceTypes": self._desc_instance_types,
        }

    def handle(self, req: Request, path: str) -> Response:
        p = self._p(req)
        action = p.get("Action", "")
        h = self._handlers().get(action)
        if not h:
            return error_response("InvalidAction", "Unknown action: " + action, 400)
        return h(p)

    def _p(self, req):
        params = dict(req.args)
        if req.content_type and "form" in req.content_type:
            params.update(req.form.to_dict())
        if not params.get("Action"):
            for k, v in parse_qs(req.get_data(as_text=True)).items():
                params[k] = v[0] if len(v) == 1 else v
        return params

    def _xml(self, action, content):
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<' + action + 'Response xmlns="' + _NS + '">\n'
            '  <requestId>' + _rid() + '</requestId>\n'
            + content +
            '\n</' + action + 'Response>'
        )
        return Response(body, 200, content_type="application/xml")

    # ---------- instances ----------

    def _desc_instances(self, p):
        filter_ids = _extract_list(p, "InstanceId")
        xml = "  <reservationSet>\n"
        for inst in self.instances.values():
            if filter_ids and inst["InstanceId"] not in filter_ids:
                continue
            xml += _reservation_item(inst)
        xml += "  </reservationSet>"
        return self._xml("DescribeInstances", xml)

    def _run_instances(self, p):
        c = get_config()
        img = p.get("ImageId", "ami-00000000")
        itype = p.get("InstanceType", "t2.micro")
        count = int(p.get("MaxCount", p.get("MinCount", 1)))
        reservation_id = "r-" + uuid.uuid4().hex[:17]
        instances_xml = ""
        for _ in range(count):
            iid = _iid()
            subnet_id = next(iter(self.subnets.keys()), "")
            sg_id = next(iter(self.security_groups.keys()), "")
            self.instances[iid] = {
                "InstanceId": iid, "ImageId": img,
                "InstanceType": itype, "State": "running",
                "StateCode": 16, "SubnetId": subnet_id,
                "SecurityGroupId": sg_id, "LaunchTime": time.time(),
                "OwnerId": c.account_id,
                "PrivateIpAddress": "10.0.0." + str(len(self.instances) % 254 + 1),
            }
            logger.info("RunInstances: %s (%s)", iid, itype)
            instances_xml += _instance_item(self.instances[iid])
        xml = (
            "  <reservationId>" + reservation_id + "</reservationId>\n"
            "  <ownerId>" + c.account_id + "</ownerId>\n"
            "  <instancesSet>\n" + instances_xml + "  </instancesSet>\n"
        )
        return self._xml("RunInstances", xml)

    def _terminate(self, p):
        ids = _extract_list(p, "InstanceId")
        xml = "  <instancesSet>\n"
        for iid in ids:
            inst = self.instances.get(iid)
            if inst:
                prev = inst["StateCode"]
                inst["State"] = "terminated"
                inst["StateCode"] = 48
                xml += _state_change(iid, prev, 48)
        xml += "  </instancesSet>"
        return self._xml("TerminateInstances", xml)

    def _start_instances(self, p):
        ids = _extract_list(p, "InstanceId")
        xml = "  <instancesSet>\n"
        for iid in ids:
            inst = self.instances.get(iid)
            if inst:
                prev = inst["StateCode"]
                inst["State"] = "running"
                inst["StateCode"] = 16
                xml += _state_change(iid, prev, 16)
        xml += "  </instancesSet>"
        return self._xml("StartInstances", xml)

    def _stop_instances(self, p):
        ids = _extract_list(p, "InstanceId")
        xml = "  <instancesSet>\n"
        for iid in ids:
            inst = self.instances.get(iid)
            if inst:
                prev = inst["StateCode"]
                inst["State"] = "stopped"
                inst["StateCode"] = 80
                xml += _state_change(iid, prev, 80)
        xml += "  </instancesSet>"
        return self._xml("StopInstances", xml)

    def _instance_status(self, p):
        xml = "  <instanceStatusSet>\n"
        for inst in self.instances.values():
            if inst["StateCode"] != 16:
                continue
            xml += (
                "    <item>"
                "<instanceId>" + inst["InstanceId"] + "</instanceId>"
                "<instanceState><code>16</code><name>running</name></instanceState>"
                "<instanceStatus><status>ok</status></instanceStatus>"
                "<systemStatus><status>ok</status></systemStatus>"
                "</item>\n"
            )
        xml += "  </instanceStatusSet>"
        return self._xml("DescribeInstanceStatus", xml)

    # ---------- VPCs / subnets ----------

    def _desc_vpcs(self, p):
        xml = "  <vpcSet>\n"
        for vpc in self.vpcs.values():
            xml += (
                "    <item>"
                "<vpcId>" + vpc["VpcId"] + "</vpcId>"
                "<cidrBlock>" + vpc["CidrBlock"] + "</cidrBlock>"
                "<isDefault>" + str(vpc["IsDefault"]).lower() + "</isDefault>"
                "<state>" + vpc["State"] + "</state>"
                "</item>\n"
            )
        xml += "  </vpcSet>"
        return self._xml("DescribeVpcs", xml)

    def _desc_subnets(self, p):
        xml = "  <subnetSet>\n"
        for sn in self.subnets.values():
            xml += (
                "    <item>"
                "<subnetId>" + sn["SubnetId"] + "</subnetId>"
                "<vpcId>" + sn["VpcId"] + "</vpcId>"
                "<cidrBlock>" + sn["CidrBlock"] + "</cidrBlock>"
                "<availabilityZone>" + sn["AvailabilityZone"] + "</availabilityZone>"
                "<defaultForAz>" + str(sn["DefaultForAz"]).lower() + "</defaultForAz>"
                "<state>" + sn["State"] + "</state>"
                "</item>\n"
            )
        xml += "  </subnetSet>"
        return self._xml("DescribeSubnets", xml)

    # ---------- security groups ----------

    def _desc_sgs(self, p):
        # collect any GroupId.N filter params
        filter_ids = _extract_list(p, "GroupId")
        xml = "  <securityGroupInfo>\n"
        for sg in self.security_groups.values():
            if filter_ids and sg["GroupId"] not in filter_ids:
                continue
            xml += _sg_item(sg)
        xml += "  </securityGroupInfo>"
        return self._xml("DescribeSecurityGroups", xml)

    def _create_sg(self, p):
        c = get_config()
        name = p.get("GroupName", "")
        desc = p.get("Description", "")
        vpc_id = p.get("VpcId", next(iter(self.vpcs.keys()), ""))
        sg_id = _sgid()
        self.security_groups[sg_id] = {
            "GroupId": sg_id, "GroupName": name,
            "Description": desc, "VpcId": vpc_id,
            "OwnerId": c.account_id,
            "IpPermissions": [], "IpPermissionsEgress": [],
        }
        logger.info("CreateSecurityGroup: %s (%s)", sg_id, name)
        return self._xml("CreateSecurityGroup", "  <groupId>" + sg_id + "</groupId>")

    def _delete_sg(self, p):
        sg_id = p.get("GroupId", "")
        self.security_groups.pop(sg_id, None)
        return self._xml("DeleteSecurityGroup", "  <return>true</return>")

    def _auth_sg_ingress(self, p):
        sg_id = p.get("GroupId", "")
        sg = self.security_groups.get(sg_id)
        if sg:
            sg["IpPermissions"].append(_extract_perm(p))
        return self._xml("AuthorizeSecurityGroupIngress", "  <return>true</return>")

    def _auth_sg_egress(self, p):
        sg_id = p.get("GroupId", "")
        sg = self.security_groups.get(sg_id)
        if sg:
            sg["IpPermissionsEgress"].append(_extract_perm(p))
        return self._xml("AuthorizeSecurityGroupEgress", "  <return>true</return>")

    # ---------- key pairs ----------

    def _desc_keypairs(self, p):
        xml = "  <keySet>\n"
        for kp in self.key_pairs.values():
            xml += (
                "    <item>"
                "<keyName>" + kp["KeyName"] + "</keyName>"
                "<keyFingerprint>" + kp["KeyFingerprint"] + "</keyFingerprint>"
                "</item>\n"
            )
        xml += "  </keySet>"
        return self._xml("DescribeKeyPairs", xml)

    def _create_keypair(self, p):
        name = p.get("KeyName", "")
        fp = uuid.uuid4().hex
        self.key_pairs[name] = {
            "KeyName": name,
            "KeyFingerprint": fp,
            "KeyMaterial": "-----BEGIN RSA PRIVATE KEY-----\nFAKEKEY\n-----END RSA PRIVATE KEY-----",
        }
        kp = self.key_pairs[name]
        return self._xml("CreateKeyPair",
            "  <keyName>" + kp["KeyName"] + "</keyName>"
            "<keyFingerprint>" + kp["KeyFingerprint"] + "</keyFingerprint>"
            "<keyMaterial>" + kp["KeyMaterial"] + "</keyMaterial>")

    def _delete_keypair(self, p):
        self.key_pairs.pop(p.get("KeyName", ""), None)
        return self._xml("DeleteKeyPair", "  <return>true</return>")

    # ---------- images ----------

    def _desc_images(self, p):
        img_id = p.get("ImageId.1", p.get("ImageId", ""))
        xml = "  <imagesSet>\n"
        if img_id:
            if img_id not in self.images:
                self.images[img_id] = {
                    "ImageId": img_id, "Name": "stub-ami",
                    "State": "available", "OwnerId": "amazon",
                }
            img = self.images[img_id]
            xml += _image_item(img)
        else:
            for img in self.images.values():
                xml += _image_item(img)
        xml += "  </imagesSet>"
        return self._xml("DescribeImages", xml)

    # ---------- regions / AZs ----------

    def _desc_regions(self, p):
        xml = "  <regionInfo>\n"
        for r in _REGIONS:
            xml += "    <item><regionName>" + r + "</regionName><regionEndpoint>ec2." + r + ".amazonaws.com</regionEndpoint></item>\n"
        xml += "  </regionInfo>"
        return self._xml("DescribeRegions", xml)

    def _desc_azs(self, p):
        c = get_config()
        zones = [c.region + "a", c.region + "b", c.region + "c"]
        xml = "  <availabilityZoneInfo>\n"
        for z in zones:
            xml += "    <item><zoneName>" + z + "</zoneName><zoneState>available</zoneState><regionName>" + c.region + "</regionName></item>\n"
        xml += "  </availabilityZoneInfo>"
        return self._xml("DescribeAvailabilityZones", xml)

    # ---------- volumes ----------

    def _desc_volumes(self, p):
        filter_ids = _extract_list(p, "VolumeId")
        xml = "  <volumeSet>\n"
        for vol in self.volumes.values():
            if filter_ids and vol["VolumeId"] not in filter_ids:
                continue
            xml += _vol_item(vol)
        xml += "  </volumeSet>"
        return self._xml("DescribeVolumes", xml)

    def _create_volume(self, p):
        c = get_config()
        vid = _volid()
        az = p.get("AvailabilityZone", c.region + "a")
        size = p.get("Size", "20")
        self.volumes[vid] = {
            "VolumeId": vid, "Size": size,
            "AvailabilityZone": az, "State": "available",
            "VolumeType": p.get("VolumeType", "gp2"),
            "CreateTime": time.time(), "Encrypted": False,
        }
        logger.info("CreateVolume: %s (%s GiB)", vid, size)
        vol = self.volumes[vid]
        xml = (
            "  <volumeId>" + vol["VolumeId"] + "</volumeId>\n"
            "  <size>" + str(vol["Size"]) + "</size>\n"
            "  <availabilityZone>" + vol["AvailabilityZone"] + "</availabilityZone>\n"
            "  <status>" + vol["State"] + "</status>\n"
            "  <volumeType>" + vol["VolumeType"] + "</volumeType>\n"
            "  <encrypted>false</encrypted>\n"
        )
        return self._xml("CreateVolume", xml)

    def _delete_volume(self, p):
        self.volumes.pop(p.get("VolumeId", ""), None)
        return self._xml("DeleteVolume", "  <return>true</return>")

    # ---------- instance types ----------

    def _desc_instance_types(self, p):
        xml = "  <instanceTypeSet>\n"
        for it in _INSTANCE_TYPES:
            xml += "    <item><instanceType>" + it + "</instanceType></item>\n"
        xml += "  </instanceTypeSet>"
        return self._xml("DescribeInstanceTypes", xml)

    def reset(self):
        self.instances = {}
        self.vpcs = {}
        self.subnets = {}
        self.security_groups = {}
        self.key_pairs = {}
        self.images = {}
        self.volumes = {}
        self._init_defaults()


# ---------- XML helpers ----------

def _instance_item(inst):
    return (
        "    <item>"
        "<instanceId>" + inst["InstanceId"] + "</instanceId>"
        "<imageId>" + inst.get("ImageId", "") + "</imageId>"
        "<instanceType>" + inst.get("InstanceType", "") + "</instanceType>"
        "<instanceState><code>" + str(inst.get("StateCode", 16)) + "</code>"
        "<name>" + inst.get("State", "running") + "</name></instanceState>"
        "<privateIpAddress>" + inst.get("PrivateIpAddress", "") + "</privateIpAddress>"
        "</item>\n"
    )

def _reservation_item(inst):
    return (
        "    <item>"
        "<reservationId>r-" + inst["InstanceId"][2:] + "</reservationId>"
        "<ownerId>" + inst.get("OwnerId", "000000000000") + "</ownerId>"
        "<instancesSet>" + _instance_item(inst) + "</instancesSet>"
        "</item>\n"
    )

def _state_change(iid, prev_code, curr_code):
    prev_names = {16: "running", 48: "terminated", 80: "stopped", 0: "pending"}
    return (
        "    <item>"
        "<instanceId>" + iid + "</instanceId>"
        "<currentState><code>" + str(curr_code) + "</code><name>" + prev_names.get(curr_code, "") + "</name></currentState>"
        "<previousState><code>" + str(prev_code) + "</code><name>" + prev_names.get(prev_code, "") + "</name></previousState>"
        "</item>\n"
    )

def _sg_item(sg):
    return (
        "    <item>"
        "<groupId>" + sg["GroupId"] + "</groupId>"
        "<groupName>" + sg["GroupName"] + "</groupName>"
        "<groupDescription>" + sg.get("Description", "") + "</groupDescription>"
        "<vpcId>" + sg.get("VpcId", "") + "</vpcId>"
        "<ownerId>" + sg.get("OwnerId", "") + "</ownerId>"
        "<ipPermissions/><ipPermissionsEgress/>"
        "</item>\n"
    )

def _image_item(img):
    return (
        "    <item>"
        "<imageId>" + img["ImageId"] + "</imageId>"
        "<imageState>" + img.get("State", "available") + "</imageState>"
        "<name>" + img.get("Name", "") + "</name>"
        "<imageOwnerId>" + img.get("OwnerId", "amazon") + "</imageOwnerId>"
        "</item>\n"
    )

def _vol_item(vol):
    return (
        "    <item>"
        "<volumeId>" + vol["VolumeId"] + "</volumeId>"
        "<size>" + str(vol.get("Size", 20)) + "</size>"
        "<availabilityZone>" + vol.get("AvailabilityZone", "") + "</availabilityZone>"
        "<status>" + vol.get("State", "available") + "</status>"
        "<volumeType>" + vol.get("VolumeType", "gp2") + "</volumeType>"
        "<encrypted>" + str(vol.get("Encrypted", False)).lower() + "</encrypted>"
        "</item>\n"
    )

def _extract_list(p, prefix):
    ids = []
    i = 1
    while True:
        key = prefix + "." + str(i)
        val = p.get(key)
        if not val:
            break
        ids.append(val)
        i += 1
    return ids

def _extract_perm(p):
    return {
        "IpProtocol": p.get("IpPermissions.1.IpProtocol", p.get("IpProtocol", "-1")),
        "FromPort": p.get("IpPermissions.1.FromPort", p.get("FromPort", "0")),
        "ToPort": p.get("IpPermissions.1.ToPort", p.get("ToPort", "65535")),
        "IpRanges": p.get("IpPermissions.1.IpRanges.1.CidrIp", p.get("CidrIp", "0.0.0.0/0")),
    }
