"""Route53 hosted zone and record set tests."""
import uuid
import pytest


@pytest.fixture
def r53():
    import boto3
    return boto3.client(
        "route53",
        endpoint_url="http://127.0.0.1:14566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )


def _zone_name(suffix=""):
    return f"test-{uuid.uuid4().hex[:8]}{suffix}.example.com"


def test_create_hosted_zone_returns_id(r53):
    name = _zone_name()
    resp = r53.create_hosted_zone(Name=name, CallerReference=uuid.uuid4().hex)
    assert "HostedZone" in resp
    zone_id = resp["HostedZone"]["Id"]
    assert zone_id.startswith("/hostedzone/")


def test_list_zones_includes_created(r53):
    name = _zone_name()
    r53.create_hosted_zone(Name=name, CallerReference=uuid.uuid4().hex)
    zones = r53.list_hosted_zones()["HostedZones"]
    names = [z["Name"] for z in zones]
    # Route53 appends trailing dot
    assert any(name.rstrip(".") in n for n in names)


def test_get_hosted_zone_by_id(r53):
    name = _zone_name()
    create_resp = r53.create_hosted_zone(Name=name, CallerReference=uuid.uuid4().hex)
    raw_id = create_resp["HostedZone"]["Id"]  # "/hostedzone/<id>"
    zone_id = raw_id.split("/")[-1]
    resp = r53.get_hosted_zone(Id=zone_id)
    assert resp["HostedZone"]["Id"] == raw_id


def test_change_record_sets_create_a_record(r53):
    name = _zone_name()
    create_resp = r53.create_hosted_zone(Name=name, CallerReference=uuid.uuid4().hex)
    zone_id = create_resp["HostedZone"]["Id"].split("/")[-1]

    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": f"www.{name}",
                        "Type": "A",
                        "TTL": 300,
                        "ResourceRecords": [{"Value": "1.2.3.4"}],
                    },
                }
            ]
        },
    )

    records = r53.list_resource_record_sets(HostedZoneId=zone_id)["ResourceRecordSets"]
    a_records = [r for r in records if r["Type"] == "A"]
    assert len(a_records) >= 1
    assert any(rv["Value"] == "1.2.3.4" for r in a_records for rv in r["ResourceRecords"])


def test_change_record_sets_upsert_cname(r53):
    name = _zone_name()
    create_resp = r53.create_hosted_zone(Name=name, CallerReference=uuid.uuid4().hex)
    zone_id = create_resp["HostedZone"]["Id"].split("/")[-1]
    record_name = f"alias.{name}"

    # First upsert — creates
    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "UPSERT",
                    "ResourceRecordSet": {
                        "Name": record_name,
                        "Type": "CNAME",
                        "TTL": 60,
                        "ResourceRecords": [{"Value": "original.example.com"}],
                    },
                }
            ]
        },
    )

    # Second upsert — updates
    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "UPSERT",
                    "ResourceRecordSet": {
                        "Name": record_name,
                        "Type": "CNAME",
                        "TTL": 60,
                        "ResourceRecords": [{"Value": "updated.example.com"}],
                    },
                }
            ]
        },
    )

    records = r53.list_resource_record_sets(HostedZoneId=zone_id)["ResourceRecordSets"]
    cname_records = [r for r in records if r["Type"] == "CNAME"]
    assert len(cname_records) == 1
    assert cname_records[0]["ResourceRecords"][0]["Value"] == "updated.example.com"


def test_change_record_sets_delete_record(r53):
    name = _zone_name()
    create_resp = r53.create_hosted_zone(Name=name, CallerReference=uuid.uuid4().hex)
    zone_id = create_resp["HostedZone"]["Id"].split("/")[-1]
    record_name = f"del.{name}"

    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": record_name,
                        "Type": "A",
                        "TTL": 300,
                        "ResourceRecords": [{"Value": "9.9.9.9"}],
                    },
                }
            ]
        },
    )

    # Verify record exists
    records_before = r53.list_resource_record_sets(HostedZoneId=zone_id)["ResourceRecordSets"]
    assert any(r["Name"] == record_name and r["Type"] == "A" for r in records_before)

    # Now delete it
    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "DELETE",
                    "ResourceRecordSet": {
                        "Name": record_name,
                        "Type": "A",
                        "TTL": 300,
                        "ResourceRecords": [{"Value": "9.9.9.9"}],
                    },
                }
            ]
        },
    )

    records_after = r53.list_resource_record_sets(HostedZoneId=zone_id)["ResourceRecordSets"]
    assert not any(r["Name"] == record_name and r["Type"] == "A" for r in records_after)


def test_delete_hosted_zone(r53):
    name = _zone_name()
    create_resp = r53.create_hosted_zone(Name=name, CallerReference=uuid.uuid4().hex)
    zone_id = create_resp["HostedZone"]["Id"].split("/")[-1]

    r53.delete_hosted_zone(Id=zone_id)

    zones = r53.list_hosted_zones()["HostedZones"]
    assert not any(z["Id"].split("/")[-1] == zone_id for z in zones)
