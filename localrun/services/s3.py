"""S3 service emulator."""

import json
import logging
import re
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from urllib.parse import unquote

from flask import Request, Response

from localrun.utils import (
    error_response,
    etag,
    iso_timestamp,
    md5_hex,
    new_request_id,
)

logger = logging.getLogger("localrun.s3")

S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"


def _s3_err(code, message, status=400):
    return error_response(code, message, status, xmlns=S3_NS)


@dataclass
class S3Object:
    key: str
    data: bytes
    content_type: str = "application/octet-stream"
    metadata: dict = field(default_factory=dict)
    etag: str = ""
    last_modified: str = ""
    size: int = 0
    version_id: str = ""
    # Set when a lifecycle rule matches — informational only, not enforced
    expiry_date: str = ""
    # True when this version is a delete marker (versioning)
    is_delete_marker: bool = False

    def __post_init__(self):
        if not self.etag:
            self.etag = f'"{md5_hex(self.data)}"'
        if not self.last_modified:
            self.last_modified = iso_timestamp()
        self.size = len(self.data)


class S3Service:
    def __init__(self):
        # buckets[bucket_name] = {key: S3Object}
        self.buckets: dict = {}
        self.bucket_created: dict = {}
        # in-progress multipart uploads: upload_id -> {bucket, key, parts: {part_num: bytes}}
        self.uploads: dict = {}

        # versioning: bucket_name -> "Enabled" | "Suspended" | ""
        self.bucket_versioning: dict = {}
        # bucket_versions: bucket_name -> {key: [S3Object, ...]}  (oldest first)
        self.bucket_versions: dict = {}

        # policies: bucket_name -> raw JSON string
        self.bucket_policies: dict = {}

        # ACLs: bucket_name -> canned ACL string, (bucket, key) -> canned ACL string
        self.bucket_acls: dict = {}
        self.object_acls: dict = {}

        # lifecycle rules: bucket_name -> raw XML string
        self.bucket_lifecycle: dict = {}

        # notifications: bucket_name -> config dict
        self.bucket_notifications: dict = {}

        # presigned tokens: token -> {bucket, key, expires_at}
        self.presigned: dict = {}

        # injected by gateway after all engines are created
        self.sqs = None
        self.sns = None
        self.lambda_svc = None

    def handle(self, req: Request, path: str) -> Response:
        parts = path.strip("/").split("/", 1) if path.strip("/") else []
        bucket = parts[0] if parts else None
        key = unquote(parts[1]) if len(parts) > 1 else None

        method = req.method

        # Presigned URL creation endpoint
        if bucket == "_s3_presign" and method == "GET":
            return self._create_presigned(req)

        if not bucket:
            if method == "GET":
                return self._list_buckets()
            return _s3_err("MethodNotAllowed", "Method not allowed", 405)

        # Bucket-level
        if key is None:
            if "delete" in req.args:
                return self._delete_objects(req, bucket)
            if "location" in req.args:
                return self._get_bucket_location(bucket)
            if "versioning" in req.args:
                if method == "GET":
                    return self._get_bucket_versioning(bucket)
                if method == "PUT":
                    return self._put_bucket_versioning(req, bucket)
            if "versions" in req.args:
                return self._list_object_versions(req, bucket)
            if "policy" in req.args:
                if method == "GET":
                    return self._get_bucket_policy(bucket)
                if method == "PUT":
                    return self._put_bucket_policy(req, bucket)
                if method == "DELETE":
                    return self._delete_bucket_policy(bucket)
            if "acl" in req.args:
                if method == "GET":
                    return self._get_bucket_acl(bucket)
                if method == "PUT":
                    return self._put_bucket_acl(req, bucket)
            if "lifecycle" in req.args:
                if method == "GET":
                    return self._get_lifecycle(bucket)
                if method == "PUT":
                    return self._put_lifecycle(req, bucket)
                if method == "DELETE":
                    return self._delete_lifecycle(bucket)
            if "notification" in req.args:
                if method == "GET":
                    return self._get_notification(bucket)
                if method == "PUT":
                    return self._put_notification(req, bucket)
            if method == "PUT":
                return self._create_bucket(bucket)
            if method == "DELETE":
                return self._delete_bucket(bucket)
            if method == "GET":
                return self._list_objects(req, bucket)
            if method == "HEAD":
                return self._head_bucket(bucket)

        # Object-level
        if key is not None:
            upload_id = req.args.get("uploadId")
            part_number = req.args.get("partNumber")
            version_id = req.args.get("versionId")

            # Presigned token check
            token = req.args.get("token")
            amz_sig = req.args.get("X-Amz-Signature")
            if (token or amz_sig) and method == "GET":
                return self._serve_presigned(req, bucket, key, token or amz_sig)

            # ACL on object
            if "acl" in req.args:
                if method == "GET":
                    return self._get_object_acl(bucket, key)
                if method == "PUT":
                    return self._put_object_acl(req, bucket, key)

            # Multipart upload operations
            if "uploads" in req.args and method == "POST":
                return self._create_multipart_upload(req, bucket, key)
            if upload_id and part_number and method == "PUT":
                return self._upload_part(req, bucket, key, upload_id, int(part_number))
            if upload_id and method == "POST":
                return self._complete_multipart_upload(req, bucket, key, upload_id)
            if upload_id and method == "DELETE":
                return self._abort_multipart_upload(bucket, key, upload_id)

            if method == "PUT":
                copy_source = req.headers.get("x-amz-copy-source")
                if copy_source:
                    return self._copy_object(req, bucket, key, copy_source)
                return self._put_object(req, bucket, key)
            if method == "GET":
                if version_id:
                    return self._get_object_version(bucket, key, version_id)
                return self._get_object(req, bucket, key)
            if method == "DELETE":
                if version_id:
                    return self._delete_object_version(bucket, key, version_id)
                return self._delete_object(bucket, key)
            if method == "HEAD":
                return self._head_object(bucket, key)

        return _s3_err("NotImplemented", "Operation not implemented", 501)

    # ─── Bucket operations ────────────────────────────────────────────────────

    def _list_buckets(self):
        buckets_xml = ""
        for name in sorted(self.buckets.keys()):
            created = self.bucket_created.get(name, iso_timestamp())
            buckets_xml += f"\n        <Bucket><Name>{name}</Name><CreationDate>{created}</CreationDate></Bucket>"
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<ListAllMyBucketsResult xmlns="{S3_NS}">\n'
            "    <Owner><ID>000000000000</ID><DisplayName>localrun</DisplayName></Owner>\n"
            f"    <Buckets>{buckets_xml}\n    </Buckets>\n"
            "</ListAllMyBucketsResult>"
        )
        return Response(body, 200, content_type="application/xml")

    def _create_bucket(self, bucket):
        if bucket in self.buckets:
            return _s3_err("BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded.", 409)
        self.buckets[bucket] = {}
        self.bucket_created[bucket] = iso_timestamp()
        logger.info("Created bucket: %s", bucket)
        resp = Response("", 200)
        resp.headers["Location"] = f"/{bucket}"
        return resp

    def _delete_bucket(self, bucket):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        if self.buckets[bucket]:
            return _s3_err("BucketNotEmpty", "The bucket you tried to delete is not empty.", 409)
        del self.buckets[bucket]
        self.bucket_created.pop(bucket, None)
        logger.info("Deleted bucket: %s", bucket)
        return Response("", 204)

    def _head_bucket(self, bucket):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        return Response("", 200)

    def _get_bucket_location(self, bucket):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        body = f'<?xml version="1.0" encoding="UTF-8"?>\n<LocationConstraint xmlns="{S3_NS}">us-east-1</LocationConstraint>'
        return Response(body, 200, content_type="application/xml")

    # ─── Object listing ───────────────────────────────────────────────────────

    def _list_objects(self, req, bucket):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)

        prefix = req.args.get("prefix", "")
        delimiter = req.args.get("delimiter", "")
        max_keys = int(req.args.get("max-keys", "1000"))
        list_type = req.args.get("list-type", "1")
        continuation_token = req.args.get("continuation-token", "")
        marker = req.args.get("marker", "")

        objects = self.buckets[bucket]
        all_matching = []
        common_prefixes = set()

        for k in sorted(objects.keys()):
            if prefix and not k.startswith(prefix):
                continue
            if delimiter:
                rest = k[len(prefix):]
                idx = rest.find(delimiter)
                if idx >= 0:
                    common_prefixes.add(prefix + rest[:idx + len(delimiter)])
                    continue
            all_matching.append(objects[k])

        # Apply start position for pagination
        start_after = continuation_token or marker
        if start_after:
            all_matching = [o for o in all_matching if o.key > start_after]

        is_truncated = len(all_matching) > max_keys
        page = all_matching[:max_keys]

        contents_xml = ""
        for obj in page:
            contents_xml += (
                f"\n    <Contents>"
                f"<Key>{obj.key}</Key>"
                f"<LastModified>{obj.last_modified}</LastModified>"
                f"<ETag>{obj.etag}</ETag>"
                f"<Size>{obj.size}</Size>"
                f"<StorageClass>STANDARD</StorageClass>"
                f"</Contents>"
            )

        prefixes_xml = ""
        for cp in sorted(common_prefixes):
            prefixes_xml += f"\n    <CommonPrefixes><Prefix>{cp}</Prefix></CommonPrefixes>"

        next_token = page[-1].key if (is_truncated and page) else ""
        truncated_str = "true" if is_truncated else "false"

        if list_type == "2":
            next_xml = f"\n    <NextContinuationToken>{next_token}</NextContinuationToken>" if is_truncated else ""
            body = (
                f'<?xml version="1.0" encoding="UTF-8"?>\n'
                f'<ListBucketResult xmlns="{S3_NS}">\n'
                f"    <Name>{bucket}</Name><Prefix>{prefix}</Prefix>"
                f"<KeyCount>{len(page)}</KeyCount><MaxKeys>{max_keys}</MaxKeys>"
                f"<IsTruncated>{truncated_str}</IsTruncated>"
                f"{next_xml}{contents_xml}{prefixes_xml}\n</ListBucketResult>"
            )
        else:
            next_xml = f"\n    <NextMarker>{next_token}</NextMarker>" if is_truncated else ""
            body = (
                f'<?xml version="1.0" encoding="UTF-8"?>\n'
                f'<ListBucketResult xmlns="{S3_NS}">\n'
                f"    <Name>{bucket}</Name><Prefix>{prefix}</Prefix>"
                f"<MaxKeys>{max_keys}</MaxKeys><IsTruncated>{truncated_str}</IsTruncated>"
                f"{next_xml}{contents_xml}{prefixes_xml}\n</ListBucketResult>"
            )

        return Response(body, 200, content_type="application/xml")

    # ─── Object CRUD ──────────────────────────────────────────────────────────

    def _put_object(self, req, bucket, key):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        data = req.get_data()
        content_type = req.headers.get("Content-Type", "application/octet-stream")
        metadata = {}
        for header, value in req.headers:
            if header.lower().startswith("x-amz-meta-"):
                metadata[header[len("x-amz-meta-"):]] = value
        obj = S3Object(key=key, data=data, content_type=content_type, metadata=metadata)
        self._apply_lifecycle_expiry(bucket, key, obj)

        versioning = self.bucket_versioning.get(bucket, "")
        if versioning == "Enabled":
            obj.version_id = uuid.uuid4().hex
            self._store_version(bucket, key, obj)

        self.buckets[bucket][key] = obj
        logger.info("Put object: s3://%s/%s (%d bytes)", bucket, key, len(data))

        self._fire_notifications(bucket, key, "s3:ObjectCreated:Put")

        resp = Response("", 200)
        resp.headers["ETag"] = obj.etag
        if obj.version_id:
            resp.headers["x-amz-version-id"] = obj.version_id
        return resp

    def _get_object(self, req, bucket, key):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        obj = self.buckets[bucket].get(key)
        if not obj:
            return _s3_err("NoSuchKey", f"The specified key does not exist: {key}", 404)

        data = obj.data
        status = 200
        range_header = req.headers.get("Range")

        if range_header:
            # Parse Range: bytes=start-end
            m = re.match(r"bytes=(\d*)-(\d*)", range_header)
            if m:
                start = int(m.group(1)) if m.group(1) else 0
                end = int(m.group(2)) if m.group(2) else len(data) - 1
                end = min(end, len(data) - 1)
                data = obj.data[start:end + 1]
                status = 206
                resp = Response(data, status, content_type=obj.content_type)
                resp.headers["Content-Range"] = f"bytes {start}-{end}/{obj.size}"
                resp.headers["Content-Length"] = str(len(data))
                resp.headers["ETag"] = obj.etag
                resp.headers["Last-Modified"] = obj.last_modified
                for mk, mv in obj.metadata.items():
                    resp.headers[f"x-amz-meta-{mk}"] = mv
                return resp

        resp = Response(data, status, content_type=obj.content_type)
        resp.headers["ETag"] = obj.etag
        resp.headers["Content-Length"] = str(obj.size)
        resp.headers["Last-Modified"] = obj.last_modified
        if obj.version_id:
            resp.headers["x-amz-version-id"] = obj.version_id
        for mk, mv in obj.metadata.items():
            resp.headers[f"x-amz-meta-{mk}"] = mv
        return resp

    def _head_object(self, bucket, key):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        obj = self.buckets[bucket].get(key)
        if not obj:
            return _s3_err("NoSuchKey", f"The specified key does not exist: {key}", 404)
        resp = Response("", 200, content_type=obj.content_type)
        resp.headers["ETag"] = obj.etag
        resp.headers["Content-Length"] = str(obj.size)
        resp.headers["Last-Modified"] = obj.last_modified
        if obj.version_id:
            resp.headers["x-amz-version-id"] = obj.version_id
        for mk, mv in obj.metadata.items():
            resp.headers[f"x-amz-meta-{mk}"] = mv
        return resp

    def _delete_object(self, bucket, key):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)

        versioning = self.bucket_versioning.get(bucket, "")
        if versioning == "Enabled":
            marker = S3Object(key=key, data=b"", is_delete_marker=True, version_id=uuid.uuid4().hex)
            self._store_version(bucket, key, marker)
            # Remove from current view but keep versions
            self.buckets[bucket].pop(key, None)
            logger.info("Delete marker added: s3://%s/%s", bucket, key)
            self._fire_notifications(bucket, key, "s3:ObjectRemoved:Delete")
            resp = Response("", 204)
            resp.headers["x-amz-version-id"] = marker.version_id
            resp.headers["x-amz-delete-marker"] = "true"
            return resp

        self.buckets[bucket].pop(key, None)
        logger.info("Deleted object: s3://%s/%s", bucket, key)
        self._fire_notifications(bucket, key, "s3:ObjectRemoved:Delete")
        return Response("", 204)

    def _copy_object(self, req, dest_bucket, dest_key, copy_source):
        source = copy_source.lstrip("/")
        parts = source.split("/", 1)
        if len(parts) < 2:
            return _s3_err("InvalidArgument", "Invalid copy source", 400)
        src_bucket, src_key = parts[0], unquote(parts[1])
        if src_bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Source bucket '{src_bucket}' does not exist.", 404)
        src_obj = self.buckets[src_bucket].get(src_key)
        if not src_obj:
            return _s3_err("NoSuchKey", f"Source key does not exist: {src_key}", 404)
        if dest_bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Destination bucket '{dest_bucket}' does not exist.", 404)
        new_obj = S3Object(key=dest_key, data=src_obj.data, content_type=src_obj.content_type, metadata=dict(src_obj.metadata))

        versioning = self.bucket_versioning.get(dest_bucket, "")
        if versioning == "Enabled":
            new_obj.version_id = uuid.uuid4().hex
            self._store_version(dest_bucket, dest_key, new_obj)

        self.buckets[dest_bucket][dest_key] = new_obj
        logger.info("Copied s3://%s/%s -> s3://%s/%s", src_bucket, src_key, dest_bucket, dest_key)

        self._fire_notifications(dest_bucket, dest_key, "s3:ObjectCreated:Copy")

        body = (
            f'<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<CopyObjectResult>'
            f'<ETag>{new_obj.etag}</ETag>'
            f'<LastModified>{new_obj.last_modified}</LastModified>'
            f'</CopyObjectResult>'
        )
        return Response(body, 200, content_type="application/xml")

    def _delete_objects(self, req, bucket):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        try:
            root = ET.fromstring(req.get_data())
        except ET.ParseError:
            return _s3_err("MalformedXML", "The XML you provided was not well-formed.", 400)
        ns = S3_NS
        deleted_xml = ""
        obj_elems = root.findall(f".//{{{ns}}}Object")
        if not obj_elems:
            obj_elems = root.findall(".//Object")
        for obj_elem in obj_elems:
            key_elem = obj_elem.find(f"{{{ns}}}Key")
            if key_elem is None:
                key_elem = obj_elem.find("Key")
            if key_elem is not None and key_elem.text:
                k = key_elem.text
                self.buckets[bucket].pop(k, None)
                deleted_xml += f"\n    <Deleted><Key>{k}</Key></Deleted>"
        body = f'<?xml version="1.0" encoding="UTF-8"?>\n<DeleteResult xmlns="{S3_NS}">{deleted_xml}\n</DeleteResult>'
        return Response(body, 200, content_type="application/xml")

    # ─── Multipart upload ─────────────────────────────────────────────────────

    def _create_multipart_upload(self, req, bucket, key):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        upload_id = uuid.uuid4().hex
        self.uploads[upload_id] = {"bucket": bucket, "key": key, "parts": {}}
        logger.info("Started multipart upload for s3://%s/%s (id=%s)", bucket, key, upload_id)
        body = (
            f'<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<InitiateMultipartUploadResult xmlns="{S3_NS}">\n'
            f"    <Bucket>{bucket}</Bucket>\n"
            f"    <Key>{key}</Key>\n"
            f"    <UploadId>{upload_id}</UploadId>\n"
            f"</InitiateMultipartUploadResult>"
        )
        return Response(body, 200, content_type="application/xml")

    def _upload_part(self, req, bucket, key, upload_id, part_number):
        upload = self.uploads.get(upload_id)
        if not upload:
            return _s3_err("NoSuchUpload", f"Upload ID {upload_id} not found.", 404)
        data = req.get_data()
        upload["parts"][part_number] = data
        part_etag = f'"{md5_hex(data)}"'
        resp = Response("", 200)
        resp.headers["ETag"] = part_etag
        return resp

    def _complete_multipart_upload(self, req, bucket, key, upload_id):
        upload = self.uploads.get(upload_id)
        if not upload:
            return _s3_err("NoSuchUpload", f"Upload ID {upload_id} not found.", 404)
        # Concatenate parts in order
        combined = b""
        for part_num in sorted(upload["parts"].keys()):
            combined += upload["parts"][part_num]
        content_type = "application/octet-stream"
        obj = S3Object(key=key, data=combined, content_type=content_type)

        versioning = self.bucket_versioning.get(bucket, "")
        if versioning == "Enabled":
            obj.version_id = uuid.uuid4().hex
            self._store_version(bucket, key, obj)

        self.buckets[bucket][key] = obj
        del self.uploads[upload_id]
        logger.info("Completed multipart upload for s3://%s/%s (%d bytes)", bucket, key, len(combined))

        self._fire_notifications(bucket, key, "s3:ObjectCreated:CompleteMultipartUpload")

        body = (
            f'<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<CompleteMultipartUploadResult xmlns="{S3_NS}">\n'
            f"    <Bucket>{bucket}</Bucket>\n"
            f"    <Key>{key}</Key>\n"
            f"    <ETag>{obj.etag}</ETag>\n"
            f"</CompleteMultipartUploadResult>"
        )
        return Response(body, 200, content_type="application/xml")

    def _abort_multipart_upload(self, bucket, key, upload_id):
        self.uploads.pop(upload_id, None)
        return Response("", 204)

    # ─── Versioning ───────────────────────────────────────────────────────────

    def _store_version(self, bucket, key, obj):
        if bucket not in self.bucket_versions:
            self.bucket_versions[bucket] = {}
        if key not in self.bucket_versions[bucket]:
            self.bucket_versions[bucket][key] = []
        self.bucket_versions[bucket][key].append(obj)

    def _get_bucket_versioning(self, bucket):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        status = self.bucket_versioning.get(bucket, "")
        status_xml = f"\n    <Status>{status}</Status>" if status else ""
        body = (
            f'<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<VersioningConfiguration xmlns="{S3_NS}">'
            f"{status_xml}\n"
            f"</VersioningConfiguration>"
        )
        return Response(body, 200, content_type="application/xml")

    def _put_bucket_versioning(self, req, bucket):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        try:
            root = ET.fromstring(req.get_data())
        except ET.ParseError:
            return _s3_err("MalformedXML", "The XML you provided was not well-formed.", 400)
        # Try with and without namespace
        status_elem = root.find(f"{{{S3_NS}}}Status")
        if status_elem is None:
            status_elem = root.find("Status")
        status = status_elem.text.strip() if (status_elem is not None and status_elem.text) else ""
        if status not in ("Enabled", "Suspended", ""):
            return _s3_err("IllegalVersioningConfigurationException", "Invalid versioning status.", 400)
        self.bucket_versioning[bucket] = status
        logger.info("Set versioning on %s: %s", bucket, status)
        return Response("", 200)

    def _list_object_versions(self, req, bucket):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        prefix = req.args.get("prefix", "")
        versions_map = self.bucket_versions.get(bucket, {})
        versions_xml = ""
        for key, ver_list in sorted(versions_map.items()):
            if prefix and not key.startswith(prefix):
                continue
            for ver in ver_list:
                if ver.is_delete_marker:
                    versions_xml += (
                        f"\n    <DeleteMarker>"
                        f"<Key>{key}</Key>"
                        f"<VersionId>{ver.version_id}</VersionId>"
                        f"<IsLatest>false</IsLatest>"
                        f"<LastModified>{ver.last_modified}</LastModified>"
                        f"</DeleteMarker>"
                    )
                else:
                    versions_xml += (
                        f"\n    <Version>"
                        f"<Key>{key}</Key>"
                        f"<VersionId>{ver.version_id}</VersionId>"
                        f"<IsLatest>false</IsLatest>"
                        f"<LastModified>{ver.last_modified}</LastModified>"
                        f"<ETag>{ver.etag}</ETag>"
                        f"<Size>{ver.size}</Size>"
                        f"<StorageClass>STANDARD</StorageClass>"
                        f"</Version>"
                    )
        body = (
            f'<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<ListVersionsResult xmlns="{S3_NS}">\n'
            f"    <Name>{bucket}</Name>"
            f"<Prefix>{prefix}</Prefix>"
            f"<IsTruncated>false</IsTruncated>"
            f"{versions_xml}\n</ListVersionsResult>"
        )
        return Response(body, 200, content_type="application/xml")

    def _get_object_version(self, bucket, key, version_id):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        versions = self.bucket_versions.get(bucket, {}).get(key, [])
        for ver in versions:
            if ver.version_id == version_id:
                if ver.is_delete_marker:
                    return _s3_err("MethodNotAllowed", "The specified method is not allowed against this resource.", 405)
                resp = Response(ver.data, 200, content_type=ver.content_type)
                resp.headers["ETag"] = ver.etag
                resp.headers["Content-Length"] = str(ver.size)
                resp.headers["Last-Modified"] = ver.last_modified
                resp.headers["x-amz-version-id"] = ver.version_id
                for mk, mv in ver.metadata.items():
                    resp.headers[f"x-amz-meta-{mk}"] = mv
                return resp
        return _s3_err("NoSuchVersion", "The specified version does not exist.", 404)

    def _delete_object_version(self, bucket, key, version_id):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        versions = self.bucket_versions.get(bucket, {}).get(key, [])
        new_versions = []
        for v in versions:
            if v.version_id != version_id:
                new_versions.append(v)
        if len(new_versions) == len(versions):
            return _s3_err("NoSuchVersion", "The specified version does not exist.", 404)
        self.bucket_versions[bucket][key] = new_versions
        # If the current latest was this version, update the bucket view
        current = self.buckets[bucket].get(key)
        if current and current.version_id == version_id:
            if new_versions and not new_versions[-1].is_delete_marker:
                self.buckets[bucket][key] = new_versions[-1]
            else:
                self.buckets[bucket].pop(key, None)
        logger.info("Deleted version %s of s3://%s/%s", version_id, bucket, key)
        resp = Response("", 204)
        resp.headers["x-amz-version-id"] = version_id
        return resp

    # ─── Bucket policies ──────────────────────────────────────────────────────

    def _get_bucket_policy(self, bucket):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        policy = self.bucket_policies.get(bucket)
        if policy is None:
            return _s3_err("NoSuchBucketPolicy", "The bucket policy does not exist.", 404)
        return Response(policy, 200, content_type="application/json")

    def _put_bucket_policy(self, req, bucket):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        body = req.get_data(as_text=True)
        try:
            json.loads(body)
        except ValueError:
            return _s3_err("MalformedPolicy", "Policies must be valid JSON.", 400)
        self.bucket_policies[bucket] = body
        logger.info("Set policy on bucket: %s", bucket)
        return Response("", 204)

    def _delete_bucket_policy(self, bucket):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        self.bucket_policies.pop(bucket, None)
        return Response("", 204)

    # ─── ACLs ─────────────────────────────────────────────────────────────────

    def _acl_xml(self, acl):
        # Returns a minimal XML representation for canned ACLs
        grants = ""
        if acl in ("public-read", "public-read-write"):
            grants = (
                "\n    <Grant>"
                '<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">'
                "<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>"
                "</Grantee>"
                "<Permission>READ</Permission>"
                "</Grant>"
            )
        if acl == "public-read-write":
            grants += (
                "\n    <Grant>"
                '<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">'
                "<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>"
                "</Grantee>"
                "<Permission>WRITE</Permission>"
                "</Grant>"
            )
        body = (
            f'<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<AccessControlPolicy xmlns="{S3_NS}">\n'
            f"  <Owner><ID>000000000000</ID><DisplayName>localrun</DisplayName></Owner>\n"
            f"  <AccessControlList>{grants}\n  </AccessControlList>\n"
            f"</AccessControlPolicy>"
        )
        return body

    def _get_bucket_acl(self, bucket):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        acl = self.bucket_acls.get(bucket, "private")
        return Response(self._acl_xml(acl), 200, content_type="application/xml")

    def _put_bucket_acl(self, req, bucket):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        acl = req.headers.get("x-amz-acl", "private")
        self.bucket_acls[bucket] = acl
        logger.info("Set ACL on bucket %s: %s", bucket, acl)
        return Response("", 200)

    def _get_object_acl(self, bucket, key):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        if key not in self.buckets[bucket]:
            return _s3_err("NoSuchKey", f"The specified key does not exist: {key}", 404)
        acl = self.object_acls.get((bucket, key), "private")
        return Response(self._acl_xml(acl), 200, content_type="application/xml")

    def _put_object_acl(self, req, bucket, key):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        if key not in self.buckets[bucket]:
            return _s3_err("NoSuchKey", f"The specified key does not exist: {key}", 404)
        acl = req.headers.get("x-amz-acl", "private")
        self.object_acls[(bucket, key)] = acl
        logger.info("Set ACL on s3://%s/%s: %s", bucket, key, acl)
        return Response("", 200)

    # ─── Lifecycle rules ──────────────────────────────────────────────────────

    def _get_lifecycle(self, bucket):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        raw = self.bucket_lifecycle.get(bucket)
        if raw is None:
            return _s3_err("NoSuchLifecycleConfiguration", "The lifecycle configuration does not exist.", 404)
        return Response(raw, 200, content_type="application/xml")

    def _put_lifecycle(self, req, bucket):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        raw = req.get_data(as_text=True)
        try:
            ET.fromstring(raw)
        except ET.ParseError:
            return _s3_err("MalformedXML", "The XML you provided was not well-formed.", 400)
        self.bucket_lifecycle[bucket] = raw
        logger.info("Set lifecycle rules on bucket: %s", bucket)
        return Response("", 200)

    def _delete_lifecycle(self, bucket):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        self.bucket_lifecycle.pop(bucket, None)
        return Response("", 204)

    def _apply_lifecycle_expiry(self, bucket, key, obj):
        """Check lifecycle rules and annotate obj.expiry_date if a matching rule exists."""
        raw = self.bucket_lifecycle.get(bucket)
        if not raw:
            return
        try:
            root = ET.fromstring(raw)
        except ET.ParseError:
            return
        for rule in root.iter("Rule"):
            status_el = rule.find("Status")
            if status_el is None or status_el.text != "Enabled":
                continue
            prefix_el = rule.find("Prefix")
            prefix = prefix_el.text if (prefix_el is not None and prefix_el.text) else ""
            if not key.startswith(prefix):
                continue
            exp_el = rule.find("Expiration")
            if exp_el is None:
                continue
            days_el = exp_el.find("Days")
            if days_el is not None and days_el.text:
                try:
                    days = int(days_el.text)
                    expiry_ts = time.time() + days * 86400
                    obj.expiry_date = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(expiry_ts))
                except ValueError:
                    pass

    # ─── Event notifications ──────────────────────────────────────────────────

    def _get_notification(self, bucket):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        config = self.bucket_notifications.get(bucket, {})
        queue_xml = ""
        for qc in config.get("QueueConfigurations", []):
            events_xml = ""
            for e in qc.get("Events", []):
                events_xml += f"<Event>{e}</Event>"
            queue_xml += (
                f"\n  <QueueConfiguration>"
                f"<Id>{qc.get('Id', '')}</Id>"
                f"<Queue>{qc.get('QueueArn', '')}</Queue>"
                f"{events_xml}"
                f"</QueueConfiguration>"
            )
        topic_xml = ""
        for tc in config.get("TopicConfigurations", []):
            events_xml = ""
            for e in tc.get("Events", []):
                events_xml += f"<Event>{e}</Event>"
            topic_xml += (
                f"\n  <TopicConfiguration>"
                f"<Id>{tc.get('Id', '')}</Id>"
                f"<Topic>{tc.get('TopicArn', '')}</Topic>"
                f"{events_xml}"
                f"</TopicConfiguration>"
            )
        lambda_xml = ""
        for lc in config.get("LambdaFunctionConfigurations", []):
            events_xml = ""
            for e in lc.get("Events", []):
                events_xml += f"<Event>{e}</Event>"
            lambda_xml += (
                f"\n  <CloudFunctionConfiguration>"
                f"<Id>{lc.get('Id', '')}</Id>"
                f"<CloudFunction>{lc.get('LambdaFunctionArn', '')}</CloudFunction>"
                f"{events_xml}"
                f"</CloudFunctionConfiguration>"
            )
        body = (
            f'<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<NotificationConfiguration xmlns="{S3_NS}">'
            f"{queue_xml}{topic_xml}{lambda_xml}\n"
            f"</NotificationConfiguration>"
        )
        return Response(body, 200, content_type="application/xml")

    def _put_notification(self, req, bucket):
        if bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{bucket}' does not exist.", 404)
        raw = req.get_data(as_text=True)
        try:
            root = ET.fromstring(raw)
        except ET.ParseError:
            return _s3_err("MalformedXML", "The XML you provided was not well-formed.", 400)
        config = {"QueueConfigurations": [], "TopicConfigurations": [], "LambdaFunctionConfigurations": []}
        for qc in root.findall("QueueConfiguration"):
            item = {"Id": _xml_text(qc, "Id"), "QueueArn": _xml_text(qc, "Queue"), "Events": []}
            for e in qc.findall("Event"):
                if e.text:
                    item["Events"].append(e.text)
            config["QueueConfigurations"].append(item)
        for tc in root.findall("TopicConfiguration"):
            item = {"Id": _xml_text(tc, "Id"), "TopicArn": _xml_text(tc, "Topic"), "Events": []}
            for e in tc.findall("Event"):
                if e.text:
                    item["Events"].append(e.text)
            config["TopicConfigurations"].append(item)
        for lc in root.findall("CloudFunctionConfiguration"):
            item = {"Id": _xml_text(lc, "Id"), "LambdaFunctionArn": _xml_text(lc, "CloudFunction"), "Events": []}
            for e in lc.findall("Event"):
                if e.text:
                    item["Events"].append(e.text)
            config["LambdaFunctionConfigurations"].append(item)
        self.bucket_notifications[bucket] = config
        logger.info("Set notification config on bucket: %s", bucket)
        return Response("", 200)

    def _fire_notifications(self, bucket, key, event_name):
        config = self.bucket_notifications.get(bucket)
        if not config:
            return
        event = _build_s3_event(bucket, key, event_name)
        event_body = json.dumps({"Records": [event]})

        for qc in config.get("QueueConfigurations", []):
            if not _event_matches(event_name, qc.get("Events", [])):
                continue
            queue_arn = qc.get("QueueArn", "")
            self._deliver_sqs(queue_arn, event_body)

        for tc in config.get("TopicConfigurations", []):
            if not _event_matches(event_name, tc.get("Events", [])):
                continue
            topic_arn = tc.get("TopicArn", "")
            self._deliver_sns(topic_arn, event_body)

        for lc in config.get("LambdaFunctionConfigurations", []):
            if not _event_matches(event_name, lc.get("Events", [])):
                continue
            fn_arn = lc.get("LambdaFunctionArn", "")
            self._deliver_lambda(fn_arn, event_body)

    def _deliver_sqs(self, queue_arn, body):
        if not self.sqs:
            return
        queue_name = queue_arn.split(":")[-1]
        from localrun.services.sqs import SQSMessage
        queue_url = self.sqs._url(queue_name)
        q = self.sqs.queues.get(queue_url)
        if q:
            q.messages.append(SQSMessage(message_id=str(uuid.uuid4()), body=body))
            logger.info("S3 notification delivered to SQS queue: %s", queue_name)
        else:
            logger.warning("S3 notification: SQS queue not found for ARN %s", queue_arn)

    def _deliver_sns(self, topic_arn, body):
        if not self.sns:
            return
        topic = self.sns.topics.get(topic_arn)
        if topic:
            msg_id = str(uuid.uuid4())
            for sub in topic.subscriptions:
                if sub.protocol == "sqs" and self.sqs:
                    self.sns._deliver_to_sqs(topic_arn, sub.endpoint, body, msg_id, "S3 Notification")
            logger.info("S3 notification delivered to SNS topic: %s", topic_arn)
        else:
            logger.warning("S3 notification: SNS topic not found: %s", topic_arn)

    def _deliver_lambda(self, fn_arn, body):
        if not self.lambda_svc:
            return
        fn_name = fn_arn.split(":")[-1]
        fn = self.lambda_svc.functions.get(fn_name)
        if not fn:
            logger.warning("S3 notification: Lambda function not found: %s", fn_name)
            return
        # Fire async so we don't block the put/delete response
        t = threading.Thread(target=self.lambda_svc._execute_function, args=(fn, body), daemon=True)
        t.start()
        logger.info("S3 notification fired to Lambda: %s", fn_name)

    # ─── Presigned URLs ───────────────────────────────────────────────────────

    def _create_presigned(self, req):
        bucket = req.args.get("bucket")
        key = req.args.get("key")
        expires = int(req.args.get("expires", "3600"))
        if not bucket or not key:
            return _s3_err("InvalidArgument", "bucket and key are required", 400)
        token = uuid.uuid4().hex
        self.presigned[token] = {
            "bucket": bucket,
            "key": key,
            "expires_at": time.time() + expires,
        }
        return Response(json.dumps({"token": token}), 200, content_type="application/json")

    def _serve_presigned(self, req, bucket, key, token):
        entry = self.presigned.get(token)
        if not entry:
            return _s3_err("AccessDenied", "Request has expired or is invalid.", 403)
        if time.time() > entry["expires_at"]:
            self.presigned.pop(token, None)
            return _s3_err("AccessDenied", "Request has expired.", 403)
        # Ignore bucket/key from path — serve what the token says
        actual_bucket = entry["bucket"]
        actual_key = entry["key"]
        if actual_bucket not in self.buckets:
            return _s3_err("NoSuchBucket", f"Bucket '{actual_bucket}' does not exist.", 404)
        obj = self.buckets[actual_bucket].get(actual_key)
        if not obj:
            return _s3_err("NoSuchKey", f"The specified key does not exist: {actual_key}", 404)
        resp = Response(obj.data, 200, content_type=obj.content_type)
        resp.headers["ETag"] = obj.etag
        resp.headers["Content-Length"] = str(obj.size)
        resp.headers["Last-Modified"] = obj.last_modified
        for mk, mv in obj.metadata.items():
            resp.headers[f"x-amz-meta-{mk}"] = mv
        return resp

    # ─── State reset ──────────────────────────────────────────────────────────

    def reset(self):
        self.buckets = {}
        self.bucket_created = {}
        self.uploads = {}
        self.bucket_versioning = {}
        self.bucket_versions = {}
        self.bucket_policies = {}
        self.bucket_acls = {}
        self.object_acls = {}
        self.bucket_lifecycle = {}
        self.bucket_notifications = {}
        self.presigned = {}


# ─── Module-level helpers ─────────────────────────────────────────────────────

def _xml_text(elem, tag):
    child = elem.find(tag)
    return child.text if (child is not None and child.text) else ""


def _event_matches(event_name, event_patterns):
    for pattern in event_patterns:
        # Patterns may use wildcards like s3:ObjectCreated:*
        if pattern == event_name:
            return True
        if pattern.endswith("*") and event_name.startswith(pattern[:-1]):
            return True
    return False


def _build_s3_event(bucket, key, event_name):
    return {
        "eventVersion": "2.1",
        "eventSource": "aws:s3",
        "awsRegion": "us-east-1",
        "eventTime": iso_timestamp(),
        "eventName": event_name,
        "s3": {
            "s3SchemaVersion": "1.0",
            "bucket": {
                "name": bucket,
                "arn": f"arn:aws:s3:::{bucket}",
            },
            "object": {
                "key": key,
            },
        },
    }
