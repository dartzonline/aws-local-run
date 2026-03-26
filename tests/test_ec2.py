"""Tests for EC2 stub service."""
import pytest


def test_describe_regions(ec2_client):
    r = ec2_client.describe_regions()
    regions = [reg["RegionName"] for reg in r["Regions"]]
    assert "us-east-1" in regions
    assert "us-west-2" in regions


def test_describe_availability_zones(ec2_client):
    r = ec2_client.describe_availability_zones()
    assert len(r["AvailabilityZones"]) > 0


def test_default_vpc_exists(ec2_client):
    r = ec2_client.describe_vpcs()
    assert len(r["Vpcs"]) >= 1
    default_vpcs = [v for v in r["Vpcs"] if v.get("IsDefault")]
    assert len(default_vpcs) >= 1


def test_default_subnet_exists(ec2_client):
    r = ec2_client.describe_subnets()
    assert len(r["Subnets"]) >= 1


def test_default_security_group_exists(ec2_client):
    r = ec2_client.describe_security_groups()
    assert len(r["SecurityGroups"]) >= 1


def test_create_security_group(ec2_client):
    vpcs = ec2_client.describe_vpcs()["Vpcs"]
    vpc_id = vpcs[0]["VpcId"]
    r = ec2_client.create_security_group(
        GroupName="test-sg",
        Description="Test security group",
        VpcId=vpc_id,
    )
    sg_id = r["GroupId"]
    assert sg_id.startswith("sg-")

    # Describe it
    sgs = ec2_client.describe_security_groups(GroupIds=[sg_id])["SecurityGroups"]
    assert len(sgs) == 1
    assert sgs[0]["GroupName"] == "test-sg"

    # Delete it
    ec2_client.delete_security_group(GroupId=sg_id)
    sgs2 = ec2_client.describe_security_groups()["SecurityGroups"]
    ids = [sg["GroupId"] for sg in sgs2]
    assert sg_id not in ids


def test_run_and_terminate_instance(ec2_client):
    r = ec2_client.run_instances(
        ImageId="ami-12345678",
        InstanceType="t2.micro",
        MinCount=1,
        MaxCount=1,
    )
    instances = r["Instances"]
    assert len(instances) == 1
    inst_id = instances[0]["InstanceId"]
    assert inst_id.startswith("i-")

    # Check describe
    desc = ec2_client.describe_instances(InstanceIds=[inst_id])
    reservations = desc["Reservations"]
    assert len(reservations) >= 1

    # Terminate
    ec2_client.terminate_instances(InstanceIds=[inst_id])
    desc2 = ec2_client.describe_instances(InstanceIds=[inst_id])
    states = [i["State"]["Name"] for r in desc2["Reservations"] for i in r["Instances"]]
    assert "terminated" in states


def test_create_key_pair(ec2_client):
    r = ec2_client.create_key_pair(KeyName="test-key")
    assert r["KeyName"] == "test-key"
    assert "KeyMaterial" in r

    pairs = ec2_client.describe_key_pairs()["KeyPairs"]
    names = [p["KeyName"] for p in pairs]
    assert "test-key" in names

    ec2_client.delete_key_pair(KeyName="test-key")
    pairs2 = ec2_client.describe_key_pairs()["KeyPairs"]
    assert "test-key" not in [p["KeyName"] for p in pairs2]


def test_create_volume(ec2_client):
    r = ec2_client.create_volume(AvailabilityZone="us-east-1a", Size=10)
    vol_id = r["VolumeId"]
    assert vol_id.startswith("vol-")

    vols = ec2_client.describe_volumes(VolumeIds=[vol_id])["Volumes"]
    assert len(vols) == 1

    ec2_client.delete_volume(VolumeId=vol_id)


def test_instance_types(ec2_client):
    r = ec2_client.describe_instance_types(InstanceTypes=["t2.micro"])
    types = r["InstanceTypes"]
    assert len(types) >= 1
    assert types[0]["InstanceType"] == "t2.micro"
