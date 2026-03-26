"""Tests for ACM service."""
import pytest


def test_request_and_describe_certificate(acm_client):
    r = acm_client.request_certificate(DomainName="example.com")
    arn = r["CertificateArn"]
    assert "acm" in arn

    desc = acm_client.describe_certificate(CertificateArn=arn)
    cert = desc["Certificate"]
    assert cert["DomainName"] == "example.com"
    assert cert["Status"] == "ISSUED"


def test_list_certificates(acm_client):
    acm_client.request_certificate(DomainName="list-test.example.com")
    r = acm_client.list_certificates()
    domains = [c["DomainName"] for c in r["CertificateSummaryList"]]
    assert "list-test.example.com" in domains


def test_certificate_with_sans(acm_client):
    r = acm_client.request_certificate(
        DomainName="main.example.com",
        SubjectAlternativeNames=["www.example.com", "api.example.com"],
    )
    arn = r["CertificateArn"]
    desc = acm_client.describe_certificate(CertificateArn=arn)
    sans = desc["Certificate"]["SubjectAlternativeNames"]
    assert "www.example.com" in sans
    assert "api.example.com" in sans


def test_delete_certificate(acm_client):
    r = acm_client.request_certificate(DomainName="delete.example.com")
    arn = r["CertificateArn"]
    acm_client.delete_certificate(CertificateArn=arn)
    certs = acm_client.list_certificates()["CertificateSummaryList"]
    arns = [c["CertificateArn"] for c in certs]
    assert arn not in arns


def test_tag_certificate(acm_client):
    r = acm_client.request_certificate(DomainName="tag.example.com")
    arn = r["CertificateArn"]
    acm_client.add_tags_to_certificate(CertificateArn=arn, Tags=[{"Key": "env", "Value": "test"}])
    tags = acm_client.list_tags_for_certificate(CertificateArn=arn)["Tags"]
    tag_map = {t["Key"]: t["Value"] for t in tags}
    assert tag_map["env"] == "test"

    acm_client.remove_tags_from_certificate(CertificateArn=arn, Tags=[{"Key": "env", "Value": "test"}])
    tags2 = acm_client.list_tags_for_certificate(CertificateArn=arn)["Tags"]
    assert not tags2
