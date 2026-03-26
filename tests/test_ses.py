"""SES email service tests."""
import uuid
import requests

ENDPOINT = "http://127.0.0.1:14566"


def _ses_client():
    import boto3
    return boto3.client(
        "ses",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )


def test_verify_email_identity():
    ses = _ses_client()
    email = f"test-{uuid.uuid4().hex[:8]}@example.com"
    # Should not raise
    ses.verify_email_identity(EmailAddress=email)


def test_list_identities_includes_verified_email():
    ses = _ses_client()
    email = f"list-{uuid.uuid4().hex[:8]}@example.com"
    ses.verify_email_identity(EmailAddress=email)
    resp = ses.list_identities(IdentityType="EmailAddress")
    assert email in resp["Identities"]


def test_send_email():
    ses = _ses_client()
    source = f"sender-{uuid.uuid4().hex[:6]}@example.com"
    dest = f"dest-{uuid.uuid4().hex[:6]}@example.com"
    ses.verify_email_identity(EmailAddress=source)

    resp = ses.send_email(
        Source=source,
        Destination={"ToAddresses": [dest]},
        Message={
            "Subject": {"Data": "Hello from test"},
            "Body": {"Text": {"Data": "This is the body text"}},
        },
    )
    assert "MessageId" in resp


def test_send_raw_email():
    ses = _ses_client()
    source = f"raw-sender-{uuid.uuid4().hex[:6]}@example.com"
    ses.verify_email_identity(EmailAddress=source)

    raw_msg = (
        f"From: {source}\r\n"
        "To: raw-dest@example.com\r\n"
        "Subject: Raw test\r\n"
        "\r\n"
        "Plain text body\r\n"
    )
    resp = ses.send_raw_email(
        Source=source,
        RawMessage={"Data": raw_msg.encode("utf-8")},
    )
    assert "MessageId" in resp


def test_get_send_quota():
    ses = _ses_client()
    resp = ses.get_send_quota()
    assert "Max24HourSend" in resp
    assert "SentLast24Hours" in resp
    assert "MaxSendRate" in resp
    assert float(resp["Max24HourSend"]) > 0


def test_get_send_statistics():
    ses = _ses_client()
    # Send at least one email so stats are non-empty
    source = f"stats-{uuid.uuid4().hex[:6]}@example.com"
    ses.verify_email_identity(EmailAddress=source)
    ses.send_email(
        Source=source,
        Destination={"ToAddresses": ["stats-dest@example.com"]},
        Message={
            "Subject": {"Data": "Stats test"},
            "Body": {"Text": {"Data": "body"}},
        },
    )
    resp = ses.get_send_statistics()
    assert "SendDataPoints" in resp


def test_delete_identity():
    ses = _ses_client()
    email = f"del-{uuid.uuid4().hex[:8]}@example.com"
    ses.verify_email_identity(EmailAddress=email)

    identities_before = ses.list_identities(IdentityType="EmailAddress")["Identities"]
    assert email in identities_before

    ses.delete_identity(Identity=email)

    identities_after = ses.list_identities(IdentityType="EmailAddress")["Identities"]
    assert email not in identities_after


def test_ses_inbox_endpoint_captures_sent_email():
    ses = _ses_client()
    source = f"inbox-{uuid.uuid4().hex[:6]}@example.com"
    dest = f"inbox-dest-{uuid.uuid4().hex[:6]}@example.com"
    subject = f"Subject-{uuid.uuid4().hex[:6]}"
    ses.verify_email_identity(EmailAddress=source)

    ses.send_email(
        Source=source,
        Destination={"ToAddresses": [dest]},
        Message={
            "Subject": {"Data": subject},
            "Body": {"Text": {"Data": "inbox body"}},
        },
    )

    resp = requests.get(f"{ENDPOINT}/_localrun/ses/inbox", timeout=5)
    assert resp.status_code == 200
    data = resp.json()
    assert "emails" in data
    emails = data["emails"]
    assert len(emails) >= 1
    subjects = [e.get("Subject", "") for e in emails]
    assert any(subject in s for s in subjects)
