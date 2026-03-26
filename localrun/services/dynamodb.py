"""DynamoDB service emulator."""
import copy, json, logging, re, time, uuid
from dataclasses import dataclass, field
from typing import Any, Optional
from flask import Request, Response
from localrun.config import get_config
from localrun.utils import error_response, iso_timestamp, new_request_id

logger = logging.getLogger("localrun.dynamodb")

def _json_resp(data, status=200):
    return Response(json.dumps(data, default=str), status=status, content_type="application/x-amz-json-1.0")

def _json_err(code, msg, status=400):
    return Response(json.dumps({"__type": code, "message": msg}), status=status, content_type="application/x-amz-json-1.0")

class DynamoDBService:
    def __init__(self):
        self.tables = {}         # table_name -> table_meta
        self.table_items = {}    # table_name -> list of items
        self.streams = {}        # table_name -> list of stream records
        self.stream_iterators = {}  # iterator_token -> {"table": name, "pos": int}
        self.ttl_config = {}     # table_name -> {"AttributeName": str, "TimeToLiveStatus": str}

    def handle(self, req: Request, path: str) -> Response:
        target = req.headers.get("X-Amz-Target", "")
        action = target.split(".")[-1] if "." in target else ""
        body = {}
        try:
            body = json.loads(req.get_data(as_text=True) or "{}")
        except (json.JSONDecodeError, Exception):
            pass
        actions = {
            "CreateTable": self._create_table, "DeleteTable": self._delete_table,
            "ListTables": self._list_tables, "DescribeTable": self._describe_table,
            "PutItem": self._put_item, "GetItem": self._get_item,
            "DeleteItem": self._delete_item, "UpdateItem": self._update_item,
            "Query": self._query, "Scan": self._scan,
            "BatchWriteItem": self._batch_write, "BatchGetItem": self._batch_get,
            "TransactWriteItems": self._transact_write, "TransactGetItems": self._transact_get,
            "DescribeStream": self._describe_stream,
            "GetShardIterator": self._get_shard_iterator,
            "GetRecords": self._get_records,
            "ListStreams": self._list_streams,
            "UpdateTimeToLive": self._update_ttl,
            "DescribeTimeToLive": self._describe_ttl,
        }
        handler = actions.get(action)
        if not handler:
            return _json_err("UnknownOperationException", f"Unknown operation: {action}", 400)
        return handler(body)

    def _table_meta(self, name):
        t = self.tables.get(name)
        if not t: return None
        count = len(self.table_items.get(name, []))
        meta = {
            "TableName": t["TableName"], "TableStatus": "ACTIVE",
            "KeySchema": t["KeySchema"],
            "AttributeDefinitions": t["AttributeDefinitions"],
            "TableArn": t["TableArn"], "ItemCount": count,
            "TableSizeBytes": count * 100,
            "CreationDateTime": t["CreationDateTime"],
            "ProvisionedThroughput": t.get("ProvisionedThroughput", {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}),
        }
        if t.get("GlobalSecondaryIndexes"):
            meta["GlobalSecondaryIndexes"] = t["GlobalSecondaryIndexes"]
        if t.get("LocalSecondaryIndexes"):
            meta["LocalSecondaryIndexes"] = t["LocalSecondaryIndexes"]
        # Add stream info if this table has any stream records
        if name in self.streams:
            arn = t["TableArn"]
            stream_arn = f"{arn}/stream/2024-01-01T00:00:00.000"
            meta["StreamSpecification"] = {"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"}
            meta["LatestStreamArn"] = stream_arn
        return meta

    def _create_table(self, body):
        name = body.get("TableName", "")
        if not name: return _json_err("ValidationException", "TableName required")
        if name in self.tables: return _json_err("ResourceInUseException", f"Table {name} already exists")
        c = get_config()
        meta = {
            "TableName": name, "KeySchema": body.get("KeySchema", []),
            "AttributeDefinitions": body.get("AttributeDefinitions", []),
            "TableArn": f"arn:aws:dynamodb:{c.region}:{c.account_id}:table/{name}",
            "CreationDateTime": time.time(),
            "ProvisionedThroughput": body.get("ProvisionedThroughput", {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}),
        }
        if body.get("GlobalSecondaryIndexes"):
            meta["GlobalSecondaryIndexes"] = body["GlobalSecondaryIndexes"]
        if body.get("LocalSecondaryIndexes"):
            meta["LocalSecondaryIndexes"] = body["LocalSecondaryIndexes"]
        self.tables[name] = meta
        self.table_items[name] = []
        logger.info("Created table: %s", name)
        return _json_resp({"TableDescription": self._table_meta(name)})

    def _delete_table(self, body):
        name = body.get("TableName", "")
        if name not in self.tables: return _json_err("ResourceNotFoundException", f"Table {name} not found")
        meta = self._table_meta(name)
        del self.tables[name]
        self.table_items.pop(name, None)
        self.streams.pop(name, None)
        logger.info("Deleted table: %s", name)
        return _json_resp({"TableDescription": meta})

    def _list_tables(self, body):
        names = sorted(self.tables.keys())
        limit = body.get("Limit", 100)
        return _json_resp({"TableNames": names[:limit]})

    def _describe_table(self, body):
        name = body.get("TableName", "")
        if name not in self.tables: return _json_err("ResourceNotFoundException", f"Table {name} not found")
        return _json_resp({"Table": self._table_meta(name)})

    def _get_key_attrs(self, table_name):
        t = self.tables[table_name]
        return [ks["AttributeName"] for ks in t["KeySchema"]]

    def _match_key(self, item, key):
        for k, v in key.items():
            if k not in item: return False
            if item[k] != v: return False
        return True

    def _find_item_idx(self, table_name, key):
        items = self.table_items.get(table_name, [])
        for i, item in enumerate(items):
            if self._match_key(item, key): return i
        return -1

    def _item_key(self, table_name, item):
        key_attrs = self._get_key_attrs(table_name)
        return {k: item[k] for k in key_attrs if k in item}

    def _append_stream_record(self, table_name, event_name, old_item, new_item, key):
        if table_name not in self.streams:
            self.streams[table_name] = []
        record = {
            "eventID": str(uuid.uuid4()),
            "eventName": event_name,
            "dynamodb": {
                "Keys": key,
                "SequenceNumber": str(int(time.time() * 1000)),
            }
        }
        if new_item is not None:
            record["dynamodb"]["NewImage"] = new_item
        if old_item is not None:
            record["dynamodb"]["OldImage"] = old_item
        self.streams[table_name].append(record)

    def _check_condition(self, table_name, body, current_item):
        cond = body.get("ConditionExpression")
        if not cond:
            return None
        values = body.get("ExpressionAttributeValues", {})
        attr_names = body.get("ExpressionAttributeNames", {})
        # Treat missing item as empty dict for attribute_not_exists checks
        check_item = current_item if current_item is not None else {}
        if not self._eval_condition(check_item, cond, values, attr_names):
            return _json_err("ConditionalCheckFailedException", "The conditional request failed", 400)
        return None

    def _put_item(self, body):
        name = body.get("TableName", "")
        if name not in self.tables: return _json_err("ResourceNotFoundException", f"Table {name} not found")
        item = body.get("Item", {})
        key_attrs = self._get_key_attrs(name)
        key = {k: item[k] for k in key_attrs if k in item}
        idx = self._find_item_idx(name, key)
        old = self.table_items[name][idx] if idx >= 0 else None

        err = self._check_condition(name, body, old)
        if err: return err

        if idx >= 0:
            self.table_items[name][idx] = item
        else:
            self.table_items[name].append(item)

        event = "MODIFY" if old is not None else "INSERT"
        self._append_stream_record(name, event, old, item, key)

        result = {}
        if body.get("ReturnValues") == "ALL_OLD" and old:
            result["Attributes"] = old
        return _json_resp(result)

    def _get_item(self, body):
        name = body.get("TableName", "")
        if name not in self.tables: return _json_err("ResourceNotFoundException", f"Table {name} not found")
        key = body.get("Key", {})
        idx = self._find_item_idx(name, key)
        if idx < 0: return _json_resp({})
        item = self.table_items[name][idx]
        proj = body.get("ProjectionExpression")
        if proj:
            attrs = [a.strip() for a in proj.split(",")]
            item = {k: v for k, v in item.items() if k in attrs}
        return _json_resp({"Item": item})

    def _delete_item(self, body):
        name = body.get("TableName", "")
        if name not in self.tables: return _json_err("ResourceNotFoundException", f"Table {name} not found")
        key = body.get("Key", {})
        idx = self._find_item_idx(name, key)
        old = self.table_items[name][idx] if idx >= 0 else None

        err = self._check_condition(name, body, old)
        if err: return err

        if idx >= 0:
            self.table_items[name].pop(idx)
            self._append_stream_record(name, "REMOVE", old, None, key)

        result = {}
        if body.get("ReturnValues") == "ALL_OLD" and old:
            result["Attributes"] = old
        return _json_resp(result)

    def _update_item(self, body):
        name = body.get("TableName", "")
        if name not in self.tables: return _json_err("ResourceNotFoundException", f"Table {name} not found")
        key = body.get("Key", {})
        idx = self._find_item_idx(name, key)
        is_new = idx < 0
        if is_new:
            item = dict(key)
            self.table_items[name].append(item)
            idx = len(self.table_items[name]) - 1
        else:
            item = self.table_items[name][idx]

        old_item = copy.deepcopy(item)

        err = self._check_condition(name, body, None if is_new else old_item)
        if err:
            # Roll back the appended item if we added it
            if is_new:
                self.table_items[name].pop(idx)
            return err

        # Simple SET/REMOVE expression support
        expr = body.get("UpdateExpression", "")
        values = body.get("ExpressionAttributeValues", {})
        names_map = body.get("ExpressionAttributeNames", {})
        if expr.upper().startswith("SET "):
            parts = expr[4:].split(",")
            for part in parts:
                part = part.strip()
                if "=" in part:
                    lhs, rhs = part.split("=", 1)
                    lhs = lhs.strip(); rhs = rhs.strip()
                    attr = names_map.get(lhs, lhs)
                    val = values.get(rhs, {"S": rhs})
                    item[attr] = val
        elif expr.upper().startswith("REMOVE "):
            parts = expr[7:].split(",")
            for part in parts:
                attr = names_map.get(part.strip(), part.strip())
                item.pop(attr, None)
        self.table_items[name][idx] = item

        event = "INSERT" if is_new else "MODIFY"
        self._append_stream_record(name, event, None if is_new else old_item, item, key)

        result = {}
        rv = body.get("ReturnValues", "NONE")
        if rv == "ALL_NEW": result["Attributes"] = item
        elif rv == "ALL_OLD": result["Attributes"] = old_item
        return _json_resp(result)

    def _ddb_val(self, typed_val):
        """Extract a comparable Python value from a DynamoDB typed value like {"N":"42"} or {"S":"foo"}."""
        if not isinstance(typed_val, dict):
            return typed_val
        if "N" in typed_val:
            return float(typed_val["N"])
        if "S" in typed_val:
            return typed_val["S"]
        return typed_val

    def _eval_condition(self, item, cond_expr, cond_val, attr_names=None):
        """Evaluate KeyConditionExpression or FilterExpression against an item."""
        if not cond_expr: return True
        expr = cond_expr
        if attr_names:
            for placeholder, real_name in attr_names.items():
                expr = expr.replace(placeholder, real_name)

        # Split on AND (outside function calls) — simple split works for typical queries
        parts = [p.strip() for p in expr.split(" AND ")]
        for part in parts:
            if not self._eval_single_condition(item, part, cond_val):
                return False
        return True

    def _eval_single_condition(self, item, part, cond_val):
        """Evaluate one condition clause."""

        # attribute_exists(attr)
        m = re.match(r'attribute_exists\((\w+)\)', part, re.IGNORECASE)
        if m:
            return m.group(1) in item

        # attribute_not_exists(attr)
        m = re.match(r'attribute_not_exists\((\w+)\)', part, re.IGNORECASE)
        if m:
            return m.group(1) not in item

        # contains(attr, :val)
        m = re.match(r'contains\((\w+),\s*(\S+)\)', part, re.IGNORECASE)
        if m:
            attr, valref = m.group(1), m.group(2).strip()
            needle = self._ddb_val(cond_val.get(valref, {}))
            raw = item.get(attr)
            if raw is None:
                return False
            # List type: {"L": [...]}
            if isinstance(raw, dict) and "L" in raw:
                for elem in raw["L"]:
                    if self._ddb_val(elem) == needle:
                        return True
                return False
            actual = self._ddb_val(raw)
            return str(needle) in str(actual)

        # begins_with(attr, :val)
        m = re.match(r'begins_with\((\w+),\s*(\S+)\)', part, re.IGNORECASE)
        if m:
            attr, valref = m.group(1).strip(), m.group(2).strip()
            expected = self._ddb_val(cond_val.get(valref, {}))
            actual = self._ddb_val(item.get(attr, {}))
            return str(actual).startswith(str(expected))

        # BETWEEN :v1 AND :v2
        m = re.match(r'(\w+)\s+BETWEEN\s+(\S+)\s+AND\s+(\S+)', part, re.IGNORECASE)
        if m:
            attr = m.group(1).strip()
            lo = self._ddb_val(cond_val.get(m.group(2).strip(), {}))
            hi = self._ddb_val(cond_val.get(m.group(3).strip(), {}))
            actual = self._ddb_val(item.get(attr, {}))
            try:
                return lo <= actual <= hi
            except TypeError:
                return False

        # Comparison operators: >=, <=, <>, >, <, =
        for op in (">=", "<=", "<>", ">", "<", "="):
            if f" {op} " in part:
                lhs, rhs = part.split(f" {op} ", 1)
                lhs = lhs.strip(); rhs = rhs.strip()
                expected = self._ddb_val(cond_val.get(rhs))
                actual = self._ddb_val(item.get(lhs))
                try:
                    if op == "=":  return actual == expected
                    if op == "<>": return actual != expected
                    if op == ">":  return actual > expected
                    if op == "<":  return actual < expected
                    if op == ">=": return actual >= expected
                    if op == "<=": return actual <= expected
                except TypeError:
                    return False

        return True

    def _find_index(self, table_name, index_name):
        t = self.tables[table_name]
        for idx in t.get("GlobalSecondaryIndexes", []):
            if idx.get("IndexName") == index_name:
                return idx, "GSI"
        for idx in t.get("LocalSecondaryIndexes", []):
            if idx.get("IndexName") == index_name:
                return idx, "LSI"
        return None, None

    def _is_expired(self, table_name, item):
        """Return True if the item has a TTL attribute that is in the past."""
        cfg = self.ttl_config.get(table_name, {})
        if cfg.get("TimeToLiveStatus") != "ENABLED":
            return False
        attr = cfg.get("AttributeName", "")
        if not attr or attr not in item:
            return False
        val = item[attr]
        ttl_val = val.get("N")
        if ttl_val is None:
            return False
        try:
            return float(ttl_val) < time.time()
        except (ValueError, TypeError):
            return False

    def _query(self, body):
        name = body.get("TableName", "")
        if name not in self.tables: return _json_err("ResourceNotFoundException", f"Table {name} not found")
        items = [i for i in self.table_items.get(name, []) if not self._is_expired(name, i)]
        expr = body.get("KeyConditionExpression", "")
        values = body.get("ExpressionAttributeValues", {})
        attr_names = body.get("ExpressionAttributeNames", {})
        index_name = body.get("IndexName")

        if index_name:
            index_def, index_type = self._find_index(name, index_name)
            if index_def is None:
                return _json_err("ValidationException", f"Index {index_name} not found on table {name}")
            index_keys = {ks["AttributeName"] for ks in index_def.get("KeySchema", [])}
            filtered = []
            for item in items:
                # Sparse index: skip items missing any GSI key
                if index_type == "GSI":
                    has_keys = all(k in item for k in index_keys)
                    if not has_keys:
                        continue
                if self._eval_condition(item, expr, values, attr_names):
                    filtered.append(item)
            result = filtered
        else:
            result = [i for i in items if self._eval_condition(i, expr, values, attr_names)]

        filter_expr = body.get("FilterExpression")
        if filter_expr:
            result = [i for i in result if self._eval_condition(i, filter_expr, values, attr_names)]

        limit = body.get("Limit")
        if limit: result = result[:limit]
        if not body.get("ScanIndexForward", True): result.reverse()
        return _json_resp({"Items": result, "Count": len(result), "ScannedCount": len(result)})

    def _scan(self, body):
        name = body.get("TableName", "")
        if name not in self.tables: return _json_err("ResourceNotFoundException", f"Table {name} not found")
        all_items = [i for i in self.table_items.get(name, []) if not self._is_expired(name, i)]
        scanned = len(all_items)
        filter_expr = body.get("FilterExpression")
        if filter_expr:
            values = body.get("ExpressionAttributeValues", {})
            attr_names = body.get("ExpressionAttributeNames", {})
            all_items = [i for i in all_items if self._eval_condition(i, filter_expr, values, attr_names)]
        limit = body.get("Limit")
        if limit: all_items = all_items[:limit]
        return _json_resp({"Items": all_items, "Count": len(all_items), "ScannedCount": scanned})

    def _batch_write(self, body):
        requests = body.get("RequestItems", {})
        for table_name, ops in requests.items():
            if table_name not in self.tables: continue
            for op in ops:
                if "PutRequest" in op:
                    self._put_item({"TableName": table_name, "Item": op["PutRequest"]["Item"]})
                elif "DeleteRequest" in op:
                    self._delete_item({"TableName": table_name, "Key": op["DeleteRequest"]["Key"]})
        return _json_resp({"UnprocessedItems": {}})

    def _transact_write(self, body):
        # Process each operation in order — no real atomicity needed for local testing
        for item in body.get("TransactItems", []):
            if "Put" in item:
                op = item["Put"]
                self._put_item({"TableName": op.get("TableName"), "Item": op.get("Item", {})})
            elif "Delete" in item:
                op = item["Delete"]
                self._delete_item({"TableName": op.get("TableName"), "Key": op.get("Key", {})})
            elif "Update" in item:
                op = item["Update"]
                self._update_item({"TableName": op.get("TableName"), "Key": op.get("Key", {}),
                                   "UpdateExpression": op.get("UpdateExpression", ""),
                                   "ExpressionAttributeValues": op.get("ExpressionAttributeValues", {}),
                                   "ExpressionAttributeNames": op.get("ExpressionAttributeNames", {})})
        return _json_resp({})

    def _transact_get(self, body):
        responses = []
        for item in body.get("TransactItems", []):
            get = item.get("Get", {})
            table = get.get("TableName", "")
            key = get.get("Key", {})
            idx = self._find_item_idx(table, key) if table in self.tables else -1
            if idx >= 0:
                responses.append({"Item": self.table_items[table][idx]})
            else:
                responses.append({})
        return _json_resp({"Responses": responses})

    def _batch_get(self, body):
        requests = body.get("RequestItems", {})
        responses = {}
        for table_name, spec in requests.items():
            if table_name not in self.tables: continue
            keys = spec.get("Keys", [])
            items = []
            for key in keys:
                idx = self._find_item_idx(table_name, key)
                if idx >= 0: items.append(self.table_items[table_name][idx])
            responses[table_name] = items
        return _json_resp({"Responses": responses, "UnprocessedKeys": {}})

    # --- Stream operations ---

    def _stream_arn(self, table_name):
        t = self.tables.get(table_name)
        if not t: return None
        return f"{t['TableArn']}/stream/2024-01-01T00:00:00.000"

    def _describe_stream(self, body):
        stream_arn = body.get("StreamArn", "")
        # Find the table from the ARN
        table_name = None
        for name, t in self.tables.items():
            if stream_arn.startswith(t["TableArn"]):
                table_name = name
                break
        if not table_name:
            return _json_err("ResourceNotFoundException", f"Stream {stream_arn} not found")
        records = self.streams.get(table_name, [])
        shard = {
            "ShardId": f"shardId-{table_name}-0",
            "SequenceNumberRange": {
                "StartingSequenceNumber": "0",
                "EndingSequenceNumber": str(len(records)),
            }
        }
        return _json_resp({
            "StreamDescription": {
                "StreamArn": stream_arn,
                "StreamStatus": "ENABLED",
                "StreamViewType": "NEW_AND_OLD_IMAGES",
                "TableName": table_name,
                "Shards": [shard],
            }
        })

    def _get_shard_iterator(self, body):
        stream_arn = body.get("StreamArn", "")
        table_name = None
        for name, t in self.tables.items():
            if stream_arn.startswith(t["TableArn"]):
                table_name = name
                break
        if not table_name:
            return _json_err("ResourceNotFoundException", f"Stream {stream_arn} not found")
        shard_type = body.get("ShardIteratorType", "TRIM_HORIZON")
        records = self.streams.get(table_name, [])
        pos = len(records) if shard_type == "LATEST" else 0
        token = str(uuid.uuid4())
        self.stream_iterators[token] = {"table": table_name, "pos": pos}
        return _json_resp({"ShardIterator": token})

    def _get_records(self, body):
        token = body.get("ShardIterator", "")
        state = self.stream_iterators.get(token)
        if not state:
            return _json_err("ResourceNotFoundException", f"Iterator {token} not found")
        table_name = state["table"]
        pos = state["pos"]
        records = self.streams.get(table_name, [])
        limit = body.get("Limit", 1000)
        batch = records[pos:pos + limit]
        new_pos = pos + len(batch)
        new_token = str(uuid.uuid4())
        self.stream_iterators[new_token] = {"table": table_name, "pos": new_pos}
        # Old token is consumed
        del self.stream_iterators[token]
        return _json_resp({"Records": batch, "NextShardIterator": new_token})

    def _list_streams(self, body):
        table_filter = body.get("TableName")
        arns = []
        for name in sorted(self.tables.keys()):
            if table_filter and name != table_filter:
                continue
            if name in self.streams:
                arns.append({
                    "StreamArn": self._stream_arn(name),
                    "TableName": name,
                    "StreamLabel": "2024-01-01T00:00:00.000",
                })
        return _json_resp({"Streams": arns})

    def _update_ttl(self, body):
        name = body.get("TableName", "")
        spec = body.get("TimeToLiveSpecification", {})
        attr = spec.get("AttributeName", "")
        enabled = spec.get("Enabled", False)
        status = "ENABLED" if enabled else "DISABLED"
        self.ttl_config[name] = {"AttributeName": attr, "TimeToLiveStatus": status}
        return _json_resp({"TimeToLiveSpecification": {"AttributeName": attr, "Enabled": enabled, "TimeToLiveStatus": status}})

    def _describe_ttl(self, body):
        name = body.get("TableName", "")
        cfg = self.ttl_config.get(name, {"AttributeName": "", "TimeToLiveStatus": "DISABLED"})
        return _json_resp({"TimeToLiveDescription": cfg})

    def reset(self):
        self.tables = {}
        self.table_items = {}
        self.streams = {}
        self.stream_iterators = {}
        self.ttl_config = {}
