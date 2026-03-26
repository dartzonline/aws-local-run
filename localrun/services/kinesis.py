"""Kinesis service emulator."""
import base64
import json
import logging
import time
import uuid

from flask import Request, Response

from localrun.config import get_config
from localrun.utils import iso_timestamp

logger = logging.getLogger("localrun.kinesis")


def _resp(data, status=200):
    return Response(json.dumps(data, default=str), status, content_type="application/x-amz-json-1.1")


def _err(code, msg, status=400):
    return Response(json.dumps({"__type": code, "message": msg}), status, content_type="application/x-amz-json-1.1")


class KinesisService:
    def __init__(self):
        # streams: name -> stream dict
        self.streams = {}
        # iterators: token -> {stream_name, shard_id, position}
        self.iterators = {}
        # global sequence counter
        self._seq = 0

    def handle(self, req: Request, path: str) -> Response:
        target = req.headers.get("X-Amz-Target", "")
        action = target.split(".")[-1] if "." in target else ""
        try:
            body = json.loads(req.get_data(as_text=True) or "{}")
        except Exception:
            body = {}

        actions = {
            "CreateStream": self._create_stream,
            "DeleteStream": self._delete_stream,
            "DescribeStream": self._describe_stream,
            "ListStreams": self._list_streams,
            "PutRecord": self._put_record,
            "PutRecords": self._put_records,
            "GetShardIterator": self._get_shard_iterator,
            "GetRecords": self._get_records,
            "ListShards": self._list_shards,
            "MergeShards": self._stub_ok,
            "SplitShard": self._stub_ok,
        }
        handler = actions.get(action)
        if not handler:
            return _err("UnknownOperationException", f"Unknown operation: {action}")
        return handler(body)

    def _next_seq(self):
        self._seq += 1
        # Real AWS sequence numbers are 21-digit zero-padded strings
        return str(self._seq).zfill(21)

    def _shard_id(self, stream, partition_key):
        shard_count = stream["shard_count"]
        idx = hash(partition_key) % shard_count
        return f"shardId-{str(idx).zfill(12)}"

    def _make_stream(self, name, shard_count):
        c = get_config()
        arn = f"arn:aws:kinesis:{c.region}:{c.account_id}:stream/{name}"
        shards = [
            {"ShardId": f"shardId-{str(i).zfill(12)}", "SequenceNumberRange": {"StartingSequenceNumber": "0"}}
            for i in range(shard_count)
        ]
        return {
            "name": name,
            "arn": arn,
            "shard_count": shard_count,
            "status": "ACTIVE",
            "shards": shards,
            "records": [],
            "created_at": time.time(),
        }

    def _create_stream(self, body):
        name = body.get("StreamName", "")
        if not name:
            return _err("InvalidArgumentException", "StreamName required")
        if name in self.streams:
            return _err("ResourceInUseException", f"Stream {name} already exists")
        shard_count = int(body.get("ShardCount", 1))
        self.streams[name] = self._make_stream(name, shard_count)
        logger.info("Created stream: %s (%d shards)", name, shard_count)
        return Response("", 200)

    def _delete_stream(self, body):
        name = body.get("StreamName", "")
        if name not in self.streams:
            return _err("ResourceNotFoundException", f"Stream {name} not found")
        del self.streams[name]
        logger.info("Deleted stream: %s", name)
        return Response("", 200)

    def _describe_stream(self, body):
        name = body.get("StreamName", "")
        stream = self.streams.get(name)
        if not stream:
            return _err("ResourceNotFoundException", f"Stream {name} not found")
        return _resp({
            "StreamDescription": {
                "StreamName": stream["name"],
                "StreamARN": stream["arn"],
                "StreamStatus": stream["status"],
                "Shards": stream["shards"],
                "HasMoreShards": False,
                "RetentionPeriodHours": 24,
                "StreamCreationTimestamp": stream["created_at"],
            }
        })

    def _list_streams(self, body):
        names = sorted(self.streams.keys())
        limit = int(body.get("Limit", 100))
        return _resp({"StreamNames": names[:limit], "HasMoreStreams": False})

    def _put_record(self, body):
        name = body.get("StreamName", "")
        stream = self.streams.get(name)
        if not stream:
            return _err("ResourceNotFoundException", f"Stream {name} not found")
        partition_key = body.get("PartitionKey", "default")
        # Data comes in as base64-encoded string
        data_b64 = body.get("Data", "")
        try:
            data_bytes = base64.b64decode(data_b64)
        except Exception:
            data_bytes = data_b64.encode() if isinstance(data_b64, str) else b""
        seq = self._next_seq()
        shard_id = self._shard_id(stream, partition_key)
        record = {
            "SequenceNumber": seq,
            "PartitionKey": partition_key,
            "Data": data_b64,
            "ApproximateArrivalTimestamp": time.time(),
            "ShardId": shard_id,
        }
        stream["records"].append(record)
        logger.info("PutRecord to %s shard %s seq %s", name, shard_id, seq)
        return _resp({"SequenceNumber": seq, "ShardId": shard_id})

    def _put_records(self, body):
        name = body.get("StreamName", "")
        stream = self.streams.get(name)
        if not stream:
            return _err("ResourceNotFoundException", f"Stream {name} not found")
        results = []
        for entry in body.get("Records", []):
            partition_key = entry.get("PartitionKey", "default")
            data_b64 = entry.get("Data", "")
            seq = self._next_seq()
            shard_id = self._shard_id(stream, partition_key)
            record = {
                "SequenceNumber": seq,
                "PartitionKey": partition_key,
                "Data": data_b64,
                "ApproximateArrivalTimestamp": time.time(),
                "ShardId": shard_id,
            }
            stream["records"].append(record)
            results.append({"SequenceNumber": seq, "ShardId": shard_id})
        return _resp({"FailedRecordCount": 0, "Records": results})

    def _get_shard_iterator(self, body):
        name = body.get("StreamName", "")
        stream = self.streams.get(name)
        if not stream:
            return _err("ResourceNotFoundException", f"Stream {name} not found")
        shard_id = body.get("ShardId", stream["shards"][0]["ShardId"] if stream["shards"] else "shardId-000000000000")
        iterator_type = body.get("ShardIteratorType", "TRIM_HORIZON")
        starting_seq = body.get("StartingSequenceNumber", "")

        # Determine position in the records list for this shard
        shard_records = [i for i, r in enumerate(stream["records"]) if r["ShardId"] == shard_id]

        if iterator_type == "TRIM_HORIZON":
            position = shard_records[0] if shard_records else 0
        elif iterator_type == "LATEST":
            # Start after the last record
            position = shard_records[-1] + 1 if shard_records else len(stream["records"])
        elif iterator_type == "AT_SEQUENCE_NUMBER" and starting_seq:
            position = next((i for i, r in enumerate(stream["records"]) if r["SequenceNumber"] == starting_seq and r["ShardId"] == shard_id), 0)
        elif iterator_type == "AFTER_SEQUENCE_NUMBER" and starting_seq:
            idx = next((i for i, r in enumerate(stream["records"]) if r["SequenceNumber"] == starting_seq and r["ShardId"] == shard_id), -1)
            position = idx + 1
        else:
            position = 0

        token = uuid.uuid4().hex
        self.iterators[token] = {"stream_name": name, "shard_id": shard_id, "position": position}
        return _resp({"ShardIterator": token})

    def _get_records(self, body):
        token = body.get("ShardIterator", "")
        it = self.iterators.get(token)
        if not it:
            return _err("InvalidArgumentException", "Iterator expired or not found")
        stream = self.streams.get(it["stream_name"])
        if not stream:
            return _err("ResourceNotFoundException", f"Stream not found")

        limit = int(body.get("Limit", 10000))
        shard_id = it["shard_id"]
        pos = it["position"]

        # Gather records for this shard starting from pos
        results = []
        next_pos = pos
        for i in range(pos, len(stream["records"])):
            r = stream["records"][i]
            if r["ShardId"] != shard_id:
                next_pos = i + 1
                continue
            results.append({
                "SequenceNumber": r["SequenceNumber"],
                "PartitionKey": r["PartitionKey"],
                "Data": r["Data"],
                "ApproximateArrivalTimestamp": r["ApproximateArrivalTimestamp"],
            })
            next_pos = i + 1
            if len(results) >= limit:
                break

        # Advance the iterator
        new_token = uuid.uuid4().hex
        self.iterators[new_token] = {"stream_name": it["stream_name"], "shard_id": shard_id, "position": next_pos}
        # Remove the old token to avoid memory growth
        del self.iterators[token]

        return _resp({
            "Records": results,
            "NextShardIterator": new_token,
            "MillisBehindLatest": 0,
        })

    def _list_shards(self, body):
        name = body.get("StreamName", "")
        stream = self.streams.get(name)
        if not stream:
            return _err("ResourceNotFoundException", f"Stream {name} not found")
        return _resp({"Shards": stream["shards"], "NextToken": None})

    def _stub_ok(self, body):
        return Response("", 200)

    def reset(self):
        self.streams = {}
        self.iterators = {}
        self._seq = 0
