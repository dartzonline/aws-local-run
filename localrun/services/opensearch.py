"""OpenSearch service emulator.

Two API surfaces:
  - AWS control-plane: domain management (CreateDomain, DeleteDomain, etc.)
  - OpenSearch REST: index/document/search/bulk/agg operations
"""

import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from flask import Request, Response

from localrun.config import get_config
from localrun.utils import new_request_id

logger = logging.getLogger("localrun.opensearch")


def _json(data, status=200):
    return Response(
        json.dumps(data, default=str),
        status=status,
        content_type="application/json",
    )


def _err(reason: str, type_: str = "illegal_argument_exception", status: int = 400):
    return _json({"error": {"root_cause": [{"type": type_, "reason": reason}],
                             "type": type_, "reason": reason},
                  "status": status}, status)


def _aws_xml_err(code: str, msg: str, status: int = 400):
    body = (
        f'<?xml version="1.0"?>\n<ErrorResponse>\n'
        f"  <Error><Code>{code}</Code><Message>{msg}</Message></Error>\n"
        f"  <RequestId>{new_request_id()}</RequestId>\n</ErrorResponse>"
    )
    return Response(body, status, content_type="application/xml")


def _parse_body(req: Request) -> dict:
    try:
        data = req.get_data(as_text=True)
        return json.loads(data) if data else {}
    except Exception:
        return {}


@dataclass
class OSDocument:
    doc_id: str
    source: dict
    index: str
    seq_no: int = 0
    version: int = 1
    found: bool = True


@dataclass
class OSIndex:
    name: str
    mappings: dict = field(default_factory=dict)
    settings: dict = field(default_factory=dict)
    aliases: dict = field(default_factory=dict)
    docs: Dict[str, OSDocument] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    seq_counter: int = 0

    def next_seq(self) -> int:
        self.seq_counter += 1
        return self.seq_counter

    def doc_count(self) -> int:
        return len(self.docs)

    def store_size_bytes(self) -> int:
        return sum(len(json.dumps(d.source)) for d in self.docs.values())


class OpenSearchService:

    def __init__(self):
        self.domains: Dict[str, dict] = {}
        self.indices: Dict[str, OSIndex] = {}
        self.index_templates: Dict[str, dict] = {}
        self._scrolls: Dict[str, dict] = {}

    def handle(self, req: Request, path: str) -> Response:
        method = req.method.upper()
        path = path.lstrip("/")
        # ----------------------------------------------------------------
        # AWS control-plane detection
        # ----------------------------------------------------------------
        action = req.args.get("Action", "") or req.form.get("Action", "")
        if not action:
            body = _parse_body(req)
            action = body.get("Action", "")

        aws_actions = {
            "CreateDomain": self._aws_create_domain,
            "DeleteDomain": self._aws_delete_domain,
            "DescribeDomain": self._aws_describe_domain,
            "DescribeDomains": self._aws_describe_domains,
            "ListDomainNames": self._aws_list_domain_names,
            "AddTags": self._aws_add_tags,
            "ListTags": self._aws_list_tags,
            "RemoveTags": self._aws_remove_tags,
            "UpdateDomainConfig": self._aws_update_domain_config,
            "GetDomainNames": self._aws_list_domain_names,  # alias
        }
        if action in aws_actions:
            return aws_actions[action](req, _parse_body(req))

        if path.startswith("2021-01-01/") or path.startswith("2015-01-01/"):
            return self._aws_rest_control(req, path, method)

        return self._es_dispatch(req, path, method)

    def _domain_config(self, name: str) -> dict:
        c = get_config()
        return {
            "DomainName": name,
            "ARN": f"arn:aws:es:{c.region}:{c.account_id}:domain/{name}",
            "DomainId": f"{c.account_id}/{name}",
            "Created": True,
            "Deleted": False,
            "Endpoint": f"http://localhost:{c.port}",
            "Processing": False,
            "UpgradeProcessing": False,
            "ElasticsearchVersion": "OpenSearch_2.11",
            "ElasticsearchClusterConfig": {
                "InstanceType": "t3.small.search",
                "InstanceCount": 1,
                "DedicatedMasterEnabled": False,
                "ZoneAwarenessEnabled": False,
            },
            "EBSOptions": {"EBSEnabled": True, "VolumeType": "gp3", "VolumeSize": 20},
            "SnapshotOptions": {"AutomatedSnapshotStartHour": 0},
            "AccessPolicies": "",
            "AdvancedOptions": {"rest.action.multi.allow_explicit_index": "true"},
        }

    def _aws_create_domain(self, req: Request, body: dict) -> Response:
        name = body.get("DomainName") or req.args.get("DomainName", "")
        if not name:
            return _aws_xml_err("ValidationException", "DomainName is required")
        if name in self.domains:
            return _aws_xml_err("ResourceAlreadyExistsException", f"Domain {name} already exists", 409)
        cfg = self._domain_config(name)
        cfg.update({k: v for k, v in body.items() if k != "DomainName"})
        cfg["Tags"] = []
        self.domains[name] = cfg
        logger.info("Created OpenSearch domain: %s", name)
        return _json({"DomainStatus": cfg})

    def _aws_delete_domain(self, req: Request, body: dict) -> Response:
        name = body.get("DomainName") or req.args.get("DomainName", "")
        if name not in self.domains:
            return _aws_xml_err("ResourceNotFoundException", f"Domain {name} not found", 404)
        cfg = self.domains.pop(name)
        cfg["Deleted"] = True
        logger.info("Deleted OpenSearch domain: %s", name)
        return _json({"DomainStatus": cfg})

    def _aws_describe_domain(self, req: Request, body: dict) -> Response:
        name = body.get("DomainName") or req.args.get("DomainName", "")
        if name not in self.domains:
            return _aws_xml_err("ResourceNotFoundException", f"Domain {name} not found", 404)
        return _json({"DomainStatus": self.domains[name]})

    def _aws_describe_domains(self, req: Request, body: dict) -> Response:
        names = body.get("DomainNames", [])
        result = []
        for name in names:
            if name in self.domains:
                result.append(self.domains[name])
        return _json({"DomainStatusList": result})

    def _aws_list_domain_names(self, req: Request, body: dict) -> Response:
        return _json({"DomainNames": [{"DomainName": n} for n in self.domains]})

    def _aws_add_tags(self, req: Request, body: dict) -> Response:
        arn = body.get("ARN", "")
        tags = body.get("TagList", [])
        name = arn.split("/")[-1] if "/" in arn else ""
        if name not in self.domains:
            return _aws_xml_err("ResourceNotFoundException", f"Domain not found for ARN {arn}", 404)
        self.domains[name].setdefault("Tags", []).extend(tags)
        return _json({})

    def _aws_list_tags(self, req: Request, body: dict) -> Response:
        arn = body.get("ARN") or req.args.get("ARN", "")
        name = arn.split("/")[-1] if "/" in arn else ""
        if name not in self.domains:
            return _aws_xml_err("ResourceNotFoundException", f"Domain not found for ARN {arn}", 404)
        return _json({"TagList": self.domains[name].get("Tags", [])})

    def _aws_remove_tags(self, req: Request, body: dict) -> Response:
        arn = body.get("ARN", "")
        keys_to_remove = set(body.get("TagKeys", []))
        name = arn.split("/")[-1] if "/" in arn else ""
        if name not in self.domains:
            return _aws_xml_err("ResourceNotFoundException", f"Domain not found for ARN {arn}", 404)
        self.domains[name]["Tags"] = [
            t for t in self.domains[name].get("Tags", []) if t.get("Key") not in keys_to_remove
        ]
        return _json({})

    def _aws_update_domain_config(self, req: Request, body: dict) -> Response:
        name = body.get("DomainName") or req.args.get("DomainName", "")
        if name not in self.domains:
            return _aws_xml_err("ResourceNotFoundException", f"Domain {name} not found", 404)
        self.domains[name].update({k: v for k, v in body.items() if k != "DomainName"})
        return _json({"DomainConfig": self.domains[name]})

    def _aws_rest_control(self, req: Request, path: str, method: str) -> Response:
        for prefix in ("2021-01-01/", "2015-01-01/"):
            if path.startswith(prefix):
                path = path[len(prefix):]
                break
        for svc_prefix in ("opensearch/", "es/"):
            if path.startswith(svc_prefix):
                path = path[len(svc_prefix):]
                break

        body = _parse_body(req)
        parts = [p for p in path.split("/") if p]

        if parts and parts[0] == "domain":
            if len(parts) == 1:
                if method == "POST":
                    return self._aws_create_domain(req, body)
                return self._aws_list_domain_names(req, body)
            name = parts[1]
            body.setdefault("DomainName", name)
            if len(parts) == 2:
                if method == "GET":
                    return self._aws_describe_domain(req, body)
                if method == "DELETE":
                    return self._aws_delete_domain(req, body)
                if method == "POST":
                    return self._aws_update_domain_config(req, body)
            if len(parts) >= 3 and parts[2] == "tags":
                if method == "POST":
                    body.setdefault("ARN", self.domains.get(name, {}).get("ARN", name))
                    return self._aws_add_tags(req, body)
                if method == "GET":
                    arn = self.domains.get(name, {}).get("ARN", name)
                    return self._aws_list_tags(req, {"ARN": arn})

        if parts and parts[0] == "tags":
            if method == "GET":
                return self._aws_list_tags(req, body)
            if method == "POST" and len(parts) >= 2 and parts[1] == "removal":
                return self._aws_remove_tags(req, body)
            return self._aws_add_tags(req, body)

        return _json({"error": "Unknown control-plane route"}, 400)

    def _es_dispatch(self, req: Request, path: str, method: str) -> Response:
        parts = [p for p in path.split("/") if p]

        if not parts:
            return self._cluster_info(req)

        first = parts[0]

        if first == "_cluster":
            seg = parts[1] if len(parts) > 1 else ""
            if seg == "health":
                return self._cluster_health(req, parts[2] if len(parts) > 2 else None)
            if seg == "stats":
                return self._cluster_stats(req)
            if seg == "settings":
                return _json({"persistent": {}, "transient": {}})

        if first == "_cat":
            seg = parts[1] if len(parts) > 1 else ""
            if seg == "indices":
                return self._cat_indices(req)
            if seg == "health":
                return self._cat_health(req)
            if seg == "nodes":
                return self._cat_nodes(req)
            if seg == "shards":
                return self._cat_shards(req)
            if seg == "aliases":
                return self._cat_aliases(req)
            return _json([])

        if first == "_nodes":
            return self._nodes_info(req)

        if first == "_all":
            return self._cluster_health(req)

        if first == "_aliases":
            if method in ("GET", "HEAD"):
                return self._get_aliases(req)
            if method == "POST":
                return self._update_aliases(req, _parse_body(req))

        if first == "_bulk":
            return self._bulk(req, None)

        if first == "_search":
            if len(parts) >= 2 and parts[1] == "scroll":
                return self._handle_scroll(req, method)
            return self._search(req, None, _parse_body(req))

        if first == "_mget":
            return self._mget(req, None, _parse_body(req))

        if first == "_index_template":
            tmpl_name = parts[1] if len(parts) > 1 else None
            return self._index_template(req, method, tmpl_name)

        # Index-level operations
        index_name = first

        if len(parts) == 1:
            # PUT /<index>  /  DELETE /<index>  /  GET /<index>  /  HEAD /<index>
            if method == "PUT":
                return self._create_index(req, index_name, _parse_body(req))
            if method == "DELETE":
                return self._delete_index(req, index_name)
            if method in ("GET", "HEAD"):
                return self._get_index(req, index_name)

        if len(parts) >= 2:
            second = parts[1]

            if second == "_doc" or second == "_create":
                doc_id = parts[2] if len(parts) > 2 else None
                if method in ("PUT", "POST"):
                    return self._index_doc(req, index_name, doc_id, _parse_body(req))
                if method == "GET":
                    return self._get_doc(req, index_name, doc_id)
                if method == "DELETE":
                    return self._delete_doc(req, index_name, doc_id)
                if method == "HEAD":
                    return self._head_doc(req, index_name, doc_id)

            if second == "_update" and len(parts) > 2:
                return self._update_doc(req, index_name, parts[2], _parse_body(req))

            if second == "_search":
                return self._search(req, index_name, _parse_body(req))

            if second == "_bulk":
                return self._bulk(req, index_name)

            if second == "_count":
                return self._count(req, index_name, _parse_body(req))

            if second == "_mapping" or second == "_mappings":
                if method == "PUT" or method == "POST":
                    return self._put_mapping(req, index_name, _parse_body(req))
                return self._get_mapping(req, index_name)

            if second == "_settings":
                if method == "PUT":
                    return self._put_settings(req, index_name, _parse_body(req))
                return self._get_settings(req, index_name)

            if second == "_alias":
                alias_name = parts[2] if len(parts) > 2 else None
                if method == "PUT":
                    return self._put_alias(req, index_name, alias_name)
                if method == "DELETE":
                    return self._delete_alias(req, index_name, alias_name)
                return self._get_index_aliases(req, index_name)

            if second == "_delete_by_query":
                return self._delete_by_query(req, index_name, _parse_body(req))

            if second == "_update_by_query":
                return self._update_by_query(req, index_name, _parse_body(req))

            if second == "_mget":
                return self._mget(req, index_name, _parse_body(req))

            if second == "_refresh":
                return _json({"_shards": {"total": 1, "successful": 1, "failed": 0}})

            if second == "_flush":
                return _json({"_shards": {"total": 1, "successful": 1, "failed": 0}})

            if second == "_forcemerge":
                return _json({"_shards": {"total": 1, "successful": 1, "failed": 0}})

            if second == "_stats":
                return self._index_stats(req, index_name)

        return _err(f"No handler for {method} /{path}", status=400)

    def _cluster_info(self, req: Request) -> Response:
        return _json({
            "name": "localrun-node",
            "cluster_name": "localrun",
            "cluster_uuid": "localrun-uuid",
            "version": {
                "number": "2.11.0",
                "distribution": "opensearch",
                "build_type": "tar",
                "lucene_version": "9.7.0",
            },
            "tagline": "The OpenSearch Project: https://opensearch.org/",
        })

    def _cluster_health(self, req: Request, index: Optional[str] = None) -> Response:
        return _json({
            "cluster_name": "localrun",
            "status": "green",
            "timed_out": False,
            "number_of_nodes": 1,
            "number_of_data_nodes": 1,
            "active_primary_shards": len(self.indices),
            "active_shards": len(self.indices),
            "relocating_shards": 0,
            "initializing_shards": 0,
            "unassigned_shards": 0,
            "delayed_unassigned_shards": 0,
            "number_of_pending_tasks": 0,
            "number_of_in_flight_fetch": 0,
            "task_max_waiting_in_queue_millis": 0,
            "active_shards_percent_as_number": 100.0,
        })

    def _cluster_stats(self, req: Request) -> Response:
        return _json({
            "cluster_name": "localrun",
            "status": "green",
            "indices": {
                "count": len(self.indices),
                "docs": {"count": sum(i.doc_count() for i in self.indices.values()), "deleted": 0},
                "store": {"size_in_bytes": sum(i.store_size_bytes() for i in self.indices.values())},
            },
            "nodes": {"count": {"total": 1, "data": 1}},
        })

    def _nodes_info(self, req: Request) -> Response:
        return _json({
            "nodes": {
                "localrun-node-1": {
                    "name": "localrun-node-1",
                    "version": "2.11.0",
                    "roles": ["master", "data", "ingest"],
                    "http": {"publish_address": "127.0.0.1:9200"},
                }
            }
        })

    def _cat_indices(self, req: Request) -> Response:
        fmt = req.args.get("format", "text")
        rows = []
        for name, idx in self.indices.items():
            rows.append({
                "health": "green", "status": "open", "index": name,
                "pri": "1", "rep": "0",
                "docs.count": str(idx.doc_count()),
                "docs.deleted": "0",
                "store.size": f"{idx.store_size_bytes()}b",
                "pri.store.size": f"{idx.store_size_bytes()}b",
            })
        if fmt == "json" or "json" in (req.accept_mimetypes.best or ""):
            return _json(rows)
        lines = []
        for r in rows:
            lines.append(
                f"{r['health']:<8} {r['status']:<6} {r['index']:<30} "
                f"{r['pri']:<4} {r['rep']:<4} {r['docs.count']:<10} "
                f"{r['docs.deleted']:<10} {r['store.size']:<12} {r['pri.store.size']}"
            )
        return Response("\n".join(lines) + ("\n" if lines else ""), 200, content_type="text/plain")

    def _cat_health(self, req: Request) -> Response:
        return Response(
            f"epoch      timestamp cluster   status node.total node.data shards pri relo init unassign\n"
            f"{int(time.time())} 00:00:00  localrun  green  1          1         "
            f"{len(self.indices)}     {len(self.indices)}    0    0    0\n",
            200, content_type="text/plain",
        )

    def _cat_nodes(self, req: Request) -> Response:
        return Response("127.0.0.1 50 95 0 localrun-node-1 - * master\n", 200, content_type="text/plain")

    def _cat_shards(self, req: Request) -> Response:
        lines = [f"{name} 0 p STARTED {idx.doc_count()} {idx.store_size_bytes()}b 127.0.0.1 localrun-node-1"
                 for name, idx in self.indices.items()]
        return Response("\n".join(lines) + "\n", 200, content_type="text/plain")

    def _cat_aliases(self, req: Request) -> Response:
        lines = []
        for idx_name, idx in self.indices.items():
            for alias in idx.aliases:
                lines.append(f"{alias} {idx_name} - - -")
        return Response("\n".join(lines) + "\n", 200, content_type="text/plain")

    def _get_aliases(self, req: Request) -> Response:
        result = {}
        for idx_name, idx in self.indices.items():
            if idx.aliases:
                result[idx_name] = {"aliases": {a: {} for a in idx.aliases}}
        return _json(result)

    def _update_aliases(self, req: Request, body: dict) -> Response:
        for action in body.get("actions", []):
            if "add" in action:
                info = action["add"]
                idx_name = info.get("index", "")
                alias = info.get("alias", "")
                if idx_name in self.indices and alias:
                    self.indices[idx_name].aliases[alias] = {}
            elif "remove" in action:
                info = action["remove"]
                idx_name = info.get("index", "")
                alias = info.get("alias", "")
                if idx_name in self.indices:
                    self.indices[idx_name].aliases.pop(alias, None)
        return _json({"acknowledged": True})


    def _resolve_indices(self, pattern: str) -> List[str]:
        """Expand an index pattern (supports * wildcard and comma-separated)."""
        import fnmatch
        names = []
        for part in pattern.split(","):
            part = part.strip()
            if "*" in part or "?" in part:
                names.extend(n for n in self.indices if fnmatch.fnmatch(n, part))
            elif part == "_all":
                names.extend(self.indices.keys())
            else:
                for idx_name, idx in self.indices.items():
                    if part in idx.aliases and idx_name not in names:
                        names.append(idx_name)
                if part in self.indices and part not in names:
                    names.append(part)
        return names

    def _ensure_index(self, name: str) -> OSIndex:
        if name not in self.indices:
            self.indices[name] = OSIndex(name=name)
            logger.debug("Auto-created index: %s", name)
        return self.indices[name]

    def _create_index(self, req: Request, name: str, body: dict) -> Response:
        if name in self.indices:
            return _err(f"index [{name}] already exists", "resource_already_exists_exception", 400)
        idx = OSIndex(
            name=name,
            mappings=body.get("mappings", {}),
            settings=body.get("settings", {}),
            aliases=body.get("aliases", {}),
        )
        self.indices[name] = idx
        logger.info("Created index: %s", name)
        return _json({"acknowledged": True, "shards_acknowledged": True, "index": name})

    def _delete_index(self, req: Request, pattern: str) -> Response:
        names = self._resolve_indices(pattern)
        if not names:
            return _err(f"index [{pattern}] missing", "index_not_found_exception", 404)
        for n in names:
            del self.indices[n]
        logger.info("Deleted index/indices: %s", names)
        return _json({"acknowledged": True})

    def _get_index(self, req: Request, pattern: str) -> Response:
        names = self._resolve_indices(pattern)
        if not names:
            return _err(f"index [{pattern}] missing", "index_not_found_exception", 404)
        result = {}
        for n in names:
            idx = self.indices[n]
            result[n] = {
                "aliases": {a: {} for a in idx.aliases},
                "mappings": idx.mappings,
                "settings": {
                    "index": {
                        "number_of_shards": "1",
                        "number_of_replicas": "0",
                        "creation_date": str(int(idx.created_at * 1000)),
                        **idx.settings,
                    }
                },
            }
        return _json(result)

    def _index_stats(self, req: Request, pattern: str) -> Response:
        names = self._resolve_indices(pattern)
        shards_stats = {}
        for n in names:
            idx = self.indices[n]
            shards_stats[n] = {
                "primaries": {
                    "docs": {"count": idx.doc_count(), "deleted": 0},
                    "store": {"size_in_bytes": idx.store_size_bytes()},
                },
                "total": {
                    "docs": {"count": idx.doc_count(), "deleted": 0},
                    "store": {"size_in_bytes": idx.store_size_bytes()},
                },
            }
        return _json({"_shards": {"total": len(names), "successful": len(names), "failed": 0},
                      "indices": shards_stats})


    def _put_mapping(self, req: Request, name: str, body: dict) -> Response:
        idx = self._ensure_index(name)
        idx.mappings.update(body)
        return _json({"acknowledged": True})

    def _get_mapping(self, req: Request, pattern: str) -> Response:
        names = self._resolve_indices(pattern)
        result = {n: {"mappings": self.indices[n].mappings} for n in names if n in self.indices}
        return _json(result)

    def _put_settings(self, req: Request, name: str, body: dict) -> Response:
        idx = self._ensure_index(name)
        idx.settings.update(body.get("index", body))
        return _json({"acknowledged": True})

    def _get_settings(self, req: Request, pattern: str) -> Response:
        names = self._resolve_indices(pattern)
        result = {}
        for n in names:
            if n in self.indices:
                result[n] = {"settings": {"index": self.indices[n].settings}}
        return _json(result)


    def _put_alias(self, req: Request, index_name: str, alias_name: Optional[str]) -> Response:
        if index_name not in self.indices:
            return _err(f"index [{index_name}] missing", "index_not_found_exception", 404)
        if alias_name:
            self.indices[index_name].aliases[alias_name] = {}
        return _json({"acknowledged": True})

    def _delete_alias(self, req: Request, index_name: str, alias_name: Optional[str]) -> Response:
        if index_name not in self.indices:
            return _err(f"index [{index_name}] missing", "index_not_found_exception", 404)
        if alias_name:
            self.indices[index_name].aliases.pop(alias_name, None)
        return _json({"acknowledged": True})

    def _get_index_aliases(self, req: Request, index_name: str) -> Response:
        if index_name not in self.indices:
            return _err(f"index [{index_name}] missing", "index_not_found_exception", 404)
        return _json({index_name: {"aliases": {a: {} for a in self.indices[index_name].aliases}}})


    def _index_template(self, req: Request, method: str, name: Optional[str]) -> Response:
        if method == "PUT":
            if not name:
                return _err("template name required")
            self.index_templates[name] = _parse_body(req)
            return _json({"acknowledged": True})
        if method == "GET":
            if name:
                if name not in self.index_templates:
                    return _err(f"index_template [{name}] missing", "resource_not_found_exception", 404)
                return _json({"index_templates": [{"name": name, "index_template": self.index_templates[name]}]})
            return _json({"index_templates": [{"name": n, "index_template": t}
                                              for n, t in self.index_templates.items()]})
        if method == "DELETE":
            if name and name in self.index_templates:
                del self.index_templates[name]
                return _json({"acknowledged": True})
            return _err(f"index_template [{name}] missing", "resource_not_found_exception", 404)
        return _err("method not allowed", status=405)


    def _index_doc(self, req: Request, index_name: str, doc_id: Optional[str], body: dict) -> Response:
        idx = self._ensure_index(index_name)
        is_create = doc_id is None or req.path.rstrip("/").endswith("/_create")
        if not doc_id:
            doc_id = str(uuid.uuid4())
        seq = idx.next_seq()
        existing = doc_id in idx.docs
        doc = OSDocument(doc_id=doc_id, source=body, index=index_name, seq_no=seq,
                         version=idx.docs[doc_id].version + 1 if existing else 1)
        idx.docs[doc_id] = doc
        result = "updated" if existing else "created"
        logger.debug("Indexed doc %s/%s", index_name, doc_id)
        return _json({
            "_index": index_name, "_id": doc_id, "_version": doc.version,
            "result": result, "_shards": {"total": 1, "successful": 1, "failed": 0},
            "_seq_no": seq, "_primary_term": 1,
        }, status=200 if existing else 201)

    def _get_doc(self, req: Request, index_name: str, doc_id: Optional[str]) -> Response:
        if index_name not in self.indices:
            return _json({"_index": index_name, "_id": doc_id, "found": False}, 404)
        idx = self.indices[index_name]
        doc = idx.docs.get(doc_id)
        if not doc:
            return _json({"_index": index_name, "_id": doc_id, "found": False}, 404)
        return _json({"_index": index_name, "_id": doc.doc_id, "_version": doc.version,
                      "_seq_no": doc.seq_no, "_primary_term": 1, "found": True, "_source": doc.source})

    def _head_doc(self, req: Request, index_name: str, doc_id: Optional[str]) -> Response:
        if index_name not in self.indices:
            return Response("", 404)
        doc = self.indices[index_name].docs.get(doc_id)
        return Response("", 200 if doc else 404)

    def _delete_doc(self, req: Request, index_name: str, doc_id: Optional[str]) -> Response:
        if index_name not in self.indices:
            return _json({"_index": index_name, "_id": doc_id, "found": False, "result": "not_found"}, 404)
        idx = self.indices[index_name]
        if doc_id not in idx.docs:
            return _json({"_index": index_name, "_id": doc_id, "found": False, "result": "not_found"}, 404)
        doc = idx.docs.pop(doc_id)
        return _json({"_index": index_name, "_id": doc_id, "_version": doc.version + 1,
                      "result": "deleted", "_shards": {"total": 1, "successful": 1, "failed": 0}})

    def _update_doc(self, req: Request, index_name: str, doc_id: str, body: dict) -> Response:
        idx = self._ensure_index(index_name)
        existing = idx.docs.get(doc_id)
        if not existing:
            # upsert
            upsert = body.get("upsert", body.get("doc", {}))
            return self._index_doc(req, index_name, doc_id, upsert)
        doc_update = body.get("doc", {})
        existing.source.update(doc_update)
        existing.version += 1
        existing.seq_no = idx.next_seq()
        return _json({"_index": index_name, "_id": doc_id, "_version": existing.version,
                      "result": "updated", "_shards": {"total": 1, "successful": 1, "failed": 0}})


    def _bulk(self, req: Request, default_index: Optional[str]) -> Response:
        """Process NDJSON bulk requests."""
        raw = req.get_data(as_text=True) or ""
        lines = [l for l in raw.splitlines() if l.strip()]
        items = []
        i = 0
        while i < len(lines):
            try:
                action_meta = json.loads(lines[i])
            except Exception:
                i += 1
                continue
            action_name = next(iter(action_meta), None)
            meta = action_meta.get(action_name, {})
            index_name = meta.get("_index") or default_index or "default"
            doc_id = meta.get("_id")
            i += 1

            if action_name in ("index", "create"):
                if i < len(lines):
                    try:
                        body = json.loads(lines[i])
                    except Exception:
                        body = {}
                    i += 1
                else:
                    body = {}
                if not doc_id:
                    doc_id = str(uuid.uuid4())
                idx = self._ensure_index(index_name)
                existing = doc_id in idx.docs
                seq = idx.next_seq()
                idx.docs[doc_id] = OSDocument(doc_id=doc_id, source=body, index=index_name, seq_no=seq,
                                               version=idx.docs[doc_id].version + 1 if existing else 1)
                items.append({action_name: {"_index": index_name, "_id": doc_id,
                                            "result": "updated" if existing else "created",
                                            "status": 200 if existing else 201}})

            elif action_name == "delete":
                if index_name in self.indices and doc_id in self.indices[index_name].docs:
                    del self.indices[index_name].docs[doc_id]
                    items.append({"delete": {"_index": index_name, "_id": doc_id,
                                             "result": "deleted", "status": 200}})
                else:
                    items.append({"delete": {"_index": index_name, "_id": doc_id,
                                             "result": "not_found", "status": 404}})

            elif action_name == "update":
                if i < len(lines):
                    try:
                        body = json.loads(lines[i])
                    except Exception:
                        body = {}
                    i += 1
                else:
                    body = {}
                idx = self._ensure_index(index_name)
                existing = idx.docs.get(doc_id)
                if existing:
                    existing.source.update(body.get("doc", {}))
                    existing.version += 1
                    items.append({"update": {"_index": index_name, "_id": doc_id,
                                             "result": "updated", "status": 200}})
                else:
                    items.append({"update": {"_index": index_name, "_id": doc_id,
                                             "result": "not_found", "status": 404}})
            else:
                i += 1

        return _json({"took": 1, "errors": False, "items": items})


    def _match_doc(self, source: dict, query: dict) -> bool:
        """Simple in-memory query matcher (match_all, term, match, bool, range, ids)."""
        if not query or "match_all" in query:
            return True

        if "term" in query:
            for field, value in query["term"].items():
                val = value if not isinstance(value, dict) else value.get("value", value)
                actual = source
                for part in field.split("."):
                    if isinstance(actual, dict):
                        actual = actual.get(part)
                    else:
                        actual = None
                        break
                if actual != val:
                    return False
            return True

        if "terms" in query:
            for field, values in query["terms"].items():
                actual = source
                for part in field.split("."):
                    if isinstance(actual, dict):
                        actual = actual.get(part)
                    else:
                        actual = None
                        break
                if actual not in values:
                    return False
            return True

        if "match" in query:
            for field, value in query["match"].items():
                search_val = value if isinstance(value, str) else value.get("query", "")
                actual = source
                for part in field.split("."):
                    if isinstance(actual, dict):
                        actual = actual.get(part)
                    else:
                        actual = None
                        break
                if actual is None:
                    return False
                if str(search_val).lower() not in str(actual).lower():
                    return False
            return True

        if "match_phrase" in query:
            for field, value in query["match_phrase"].items():
                phrase = value if isinstance(value, str) else value.get("query", "")
                actual = source
                for part in field.split("."):
                    if isinstance(actual, dict):
                        actual = actual.get(part)
                    else:
                        actual = None
                        break
                if phrase.lower() not in str(actual or "").lower():
                    return False
            return True

        if "prefix" in query:
            for field, value in query["prefix"].items():
                prefix_val = value if isinstance(value, str) else value.get("value", "")
                actual = source
                for part in field.split("."):
                    if isinstance(actual, dict):
                        actual = actual.get(part)
                    else:
                        actual = None
                        break
                if not str(actual or "").lower().startswith(str(prefix_val).lower()):
                    return False
            return True

        if "wildcard" in query:
            import fnmatch
            for field, value in query["wildcard"].items():
                pattern = value if isinstance(value, str) else value.get("value", "")
                actual = source
                for part in field.split("."):
                    if isinstance(actual, dict):
                        actual = actual.get(part)
                    else:
                        actual = None
                        break
                if not fnmatch.fnmatch(str(actual or "").lower(), pattern.lower()):
                    return False
            return True

        if "range" in query:
            for field, bounds in query["range"].items():
                actual = source
                for part in field.split("."):
                    if isinstance(actual, dict):
                        actual = actual.get(part)
                    else:
                        actual = None
                        break
                if actual is None:
                    return False
                try:
                    actual_n = float(actual)
                    if "gt" in bounds and not actual_n > float(bounds["gt"]): return False
                    if "gte" in bounds and not actual_n >= float(bounds["gte"]): return False
                    if "lt" in bounds and not actual_n < float(bounds["lt"]): return False
                    if "lte" in bounds and not actual_n <= float(bounds["lte"]): return False
                except (TypeError, ValueError):
                    pass
            return True

        if "ids" in query:
            return False  # handled externally via doc_id comparison

        if "exists" in query:
            field = query["exists"].get("field", "")
            actual = source
            for part in field.split("."):
                if isinstance(actual, dict):
                    actual = actual.get(part)
                else:
                    actual = None
                    break
            return actual is not None

        if "bool" in query:
            bool_q = query["bool"]
            must = bool_q.get("must", [])
            must_not = bool_q.get("must_not", [])
            should = bool_q.get("should", [])
            minimum_should_match = bool_q.get("minimum_should_match", 1 if should and not must else 0)
            filter_clauses = bool_q.get("filter", [])

            if isinstance(must, dict): must = [must]
            if isinstance(must_not, dict): must_not = [must_not]
            if isinstance(should, dict): should = [should]
            if isinstance(filter_clauses, dict): filter_clauses = [filter_clauses]

            for q in must:
                if not self._match_doc(source, q): return False
            for q in must_not:
                if self._match_doc(source, q): return False
            for q in filter_clauses:
                if not self._match_doc(source, q): return False

            if should:
                matches = sum(1 for q in should if self._match_doc(source, q))
                if matches < minimum_should_match:
                    return False
            return True

        if "query_string" in query:
            qs = query["query_string"].get("query", "")
            source_str = json.dumps(source).lower()
            return qs.lower() in source_str

        if "simple_query_string" in query:
            qs = query["simple_query_string"].get("query", "")
            source_str = json.dumps(source).lower()
            return qs.lower() in source_str

        # Unknown query type — match all
        return True

    def _search(self, req: Request, index_pattern: Optional[str], body: dict) -> Response:
        # Determine which indices to search
        if index_pattern:
            index_names = self._resolve_indices(index_pattern)
        else:
            index_names = list(self.indices.keys())

        query = body.get("query", {})
        from_ = int(body.get("from", req.args.get("from", 0)))
        size = int(body.get("size", req.args.get("size", 10)))
        source_filter = body.get("_source", True)
        sort = body.get("sort", [])

        ids_query = None
        if "ids" in query:
            ids_query = set(query["ids"].get("values", []))

        all_hits = []
        for idx_name in index_names:
            if idx_name not in self.indices:
                continue
            idx = self.indices[idx_name]
            for doc_id, doc in idx.docs.items():
                if ids_query is not None:
                    if doc_id in ids_query:
                        all_hits.append(doc)
                elif self._match_doc(doc.source, query):
                    all_hits.append(doc)

        # Sorting
        if sort:
            sort_fields = []
            if isinstance(sort, list):
                for s in sort:
                    if isinstance(s, str):
                        sort_fields.append((s, "asc"))
                    elif isinstance(s, dict):
                        for k, v in s.items():
                            order = v if isinstance(v, str) else v.get("order", "asc")
                            sort_fields.append((k, order))
            for sf, order in reversed(sort_fields):
                reverse = order == "desc"
                all_hits.sort(key=lambda d: d.source.get(sf, ""), reverse=reverse)

        total = len(all_hits)
        page = all_hits[from_: from_ + size]

        # _source filtering
        def apply_source_filter(source):
            if source_filter is True or source_filter is None:
                return source
            if source_filter is False:
                return {}
            if isinstance(source_filter, list):
                return {k: v for k, v in source.items() if k in source_filter}
            if isinstance(source_filter, dict):
                includes = source_filter.get("includes", [])
                excludes = source_filter.get("excludes", [])
                result = {k: v for k, v in source.items() if (not includes or k in includes) and k not in excludes}
                return result
            return source

        hits = [
            {
                "_index": d.index, "_id": d.doc_id, "_score": 1.0,
                "_version": d.version, "_seq_no": d.seq_no, "_primary_term": 1,
                "_source": apply_source_filter(d.source),
            }
            for d in page
        ]

        scroll = req.args.get("scroll") or body.get("scroll")
        response = {
            "took": 1,
            "timed_out": False,
            "_shards": {"total": max(len(index_names), 1), "successful": max(len(index_names), 1),
                        "skipped": 0, "failed": 0},
            "hits": {
                "total": {"value": total, "relation": "eq"},
                "max_score": 1.0 if hits else None,
                "hits": hits,
            },
        }

        if scroll:
            scroll_id = str(uuid.uuid4())
            remaining = all_hits[from_ + size:]
            self._scrolls[scroll_id] = {"hits": remaining, "pos": 0, "size": size,
                                         "source_filter": source_filter}
            response["_scroll_id"] = scroll_id

        # Aggregations (basic support)
        if "aggs" in body or "aggregations" in body:
            aggs_def = body.get("aggs") or body.get("aggregations", {})
            response["aggregations"] = self._compute_aggs(all_hits, aggs_def)

        return _json(response)

    def _handle_scroll(self, req: Request, method: str) -> Response:
        if method == "DELETE":
            body = _parse_body(req)
            scroll_id = body.get("scroll_id") or req.args.get("scroll_id", "")
            if isinstance(scroll_id, list):
                for sid in scroll_id:
                    self._scrolls.pop(sid, None)
            else:
                self._scrolls.pop(scroll_id, None)
            return _json({"succeeded": True, "num_freed": 1})

        body = _parse_body(req)
        scroll_id = body.get("scroll_id") or req.args.get("scroll_id", "")
        ctx = self._scrolls.get(scroll_id)
        if not ctx:
            return _err("No search context found for scroll_id", "search_phase_execution_exception", 404)
        size = ctx["size"]
        source_filter = ctx.get("source_filter", True)
        page = ctx["hits"][ctx["pos"]: ctx["pos"] + size]
        ctx["pos"] += size

        hits = [
            {"_index": d.index, "_id": d.doc_id, "_score": 1.0,
             "_source": d.source if source_filter is True else {}}
            for d in page
        ]
        return _json({
            "_scroll_id": scroll_id,
            "took": 1, "timed_out": False,
            "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
            "hits": {"total": {"value": len(ctx["hits"]), "relation": "eq"},
                     "max_score": 1.0, "hits": hits},
        })


    def _count(self, req: Request, index_pattern: str, body: dict) -> Response:
        index_names = self._resolve_indices(index_pattern)
        query = body.get("query", {})
        total = 0
        for n in index_names:
            if n not in self.indices:
                continue
            for doc in self.indices[n].docs.values():
                if self._match_doc(doc.source, query):
                    total += 1
        return _json({"count": total, "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0}})


    def _mget(self, req: Request, default_index: Optional[str], body: dict) -> Response:
        docs_out = []
        for item in body.get("docs", []):
            idx_name = item.get("_index") or default_index or ""
            doc_id = item.get("_id", "")
            idx = self.indices.get(idx_name)
            if idx and doc_id in idx.docs:
                doc = idx.docs[doc_id]
                docs_out.append({"_index": idx_name, "_id": doc_id, "_version": doc.version,
                                  "found": True, "_source": doc.source})
            else:
                docs_out.append({"_index": idx_name, "_id": doc_id, "found": False})
        return _json({"docs": docs_out})


    def _delete_by_query(self, req: Request, index_pattern: str, body: dict) -> Response:
        index_names = self._resolve_indices(index_pattern)
        query = body.get("query", {})
        deleted = 0
        for n in index_names:
            if n not in self.indices:
                continue
            to_del = [did for did, doc in self.indices[n].docs.items()
                      if self._match_doc(doc.source, query)]
            for did in to_del:
                del self.indices[n].docs[did]
                deleted += 1
        return _json({"took": 1, "timed_out": False, "deleted": deleted,
                      "failures": [], "_shards": {"total": 1, "successful": 1, "failed": 0}})

    def _update_by_query(self, req: Request, index_pattern: str, body: dict) -> Response:
        index_names = self._resolve_indices(index_pattern)
        query = body.get("query", {})
        script = body.get("script", {})
        updated = 0
        for n in index_names:
            if n not in self.indices:
                continue
            for doc in self.indices[n].docs.values():
                if self._match_doc(doc.source, query):
                    src = script.get("source", "") or script.get("inline", "")
                    params = script.get("params", {})
                    if src and params:
                        for param_key, param_val in params.items():
                            # Replace ctx._source.<field> = params.<param>
                            for line in src.split(";"):
                                line = line.strip()
                                if "ctx._source." in line and "=" in line:
                                    field = line.split("ctx._source.")[1].split("=")[0].strip()
                                    doc.source[field] = param_val
                    doc.version += 1
                    updated += 1
        return _json({"took": 1, "timed_out": False, "updated": updated,
                      "failures": [], "_shards": {"total": 1, "successful": 1, "failed": 0}})


    def _compute_aggs(self, docs: list, aggs_def: dict) -> dict:
        """Compute basic aggregations over a list of OSDocument objects."""
        result = {}
        for agg_name, agg_body in aggs_def.items():
            if "terms" in agg_body:
                field = agg_body["terms"].get("field", "")
                size = agg_body["terms"].get("size", 10)
                counts: Dict[Any, int] = {}
                for doc in docs:
                    val = doc.source.get(field)
                    if val is not None:
                        counts[val] = counts.get(val, 0) + 1
                buckets = sorted(counts.items(), key=lambda x: -x[1])[:size]
                result[agg_name] = {
                    "buckets": [{"key": k, "doc_count": v} for k, v in buckets],
                    "doc_count_error_upper_bound": 0, "sum_other_doc_count": 0,
                }
            elif "sum" in agg_body:
                field = agg_body["sum"].get("field", "")
                total = sum(doc.source.get(field, 0) or 0 for doc in docs
                            if isinstance(doc.source.get(field), (int, float)))
                result[agg_name] = {"value": total}
            elif "avg" in agg_body:
                field = agg_body["avg"].get("field", "")
                vals = [doc.source.get(field) for doc in docs
                        if isinstance(doc.source.get(field), (int, float))]
                result[agg_name] = {"value": sum(vals) / len(vals) if vals else None}
            elif "min" in agg_body:
                field = agg_body["min"].get("field", "")
                vals = [doc.source.get(field) for doc in docs
                        if isinstance(doc.source.get(field), (int, float))]
                result[agg_name] = {"value": min(vals) if vals else None}
            elif "max" in agg_body:
                field = agg_body["max"].get("field", "")
                vals = [doc.source.get(field) for doc in docs
                        if isinstance(doc.source.get(field), (int, float))]
                result[agg_name] = {"value": max(vals) if vals else None}
            elif "value_count" in agg_body:
                field = agg_body["value_count"].get("field", "")
                count = sum(1 for doc in docs if doc.source.get(field) is not None)
                result[agg_name] = {"value": count}
            elif "cardinality" in agg_body:
                field = agg_body["cardinality"].get("field", "")
                unique = len({doc.source.get(field) for doc in docs if doc.source.get(field) is not None})
                result[agg_name] = {"value": unique}
            elif "date_histogram" in agg_body:
                field = agg_body["date_histogram"].get("field", "")
                counts: Dict[str, int] = {}
                for doc in docs:
                    val = str(doc.source.get(field, ""))
                    date_key = val[:10] if val else "unknown"  # truncate to date
                    counts[date_key] = counts.get(date_key, 0) + 1
                result[agg_name] = {
                    "buckets": [{"key_as_string": k, "key": k, "doc_count": v}
                                for k, v in sorted(counts.items())]
                }
            else:
                result[agg_name] = {}
        return result

    def reset(self):
        self.domains = {}
        self.indices = {}
        self.index_templates = {}
        self._scrolls = {}
