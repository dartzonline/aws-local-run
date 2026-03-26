"""OpenSearch service tests — control-plane (domain management) and data-plane (REST API)."""
import json
import pytest
import requests

BASE = "http://127.0.0.1:14566"




def _req(method, path, **kwargs):
    """Fire a request at the LocalRun server."""
    url = f"{BASE}/{path.lstrip('/')}"
    resp = getattr(requests, method.lower())(url, **kwargs)
    return resp


def _json_req(method, path, body=None, **kwargs):
    headers = kwargs.pop("headers", {})
    headers.setdefault("Content-Type", "application/json")
    return _req(method, path, json=body, headers=headers, **kwargs)




class TestOpenSearchDomainManagement:

    def test_create_domain(self):
        r = _json_req("POST", "2021-01-01/opensearch/domain",
                      body={"DomainName": "test-domain"})
        assert r.status_code == 200
        data = r.json()
        assert data["DomainStatus"]["DomainName"] == "test-domain"
        assert "ARN" in data["DomainStatus"]

    def test_create_domain_duplicate_fails(self):
        _json_req("POST", "2021-01-01/opensearch/domain",
                  body={"DomainName": "dup-domain"})
        r = _json_req("POST", "2021-01-01/opensearch/domain",
                      body={"DomainName": "dup-domain"})
        assert r.status_code == 409

    def test_describe_domain(self):
        _json_req("POST", "2021-01-01/opensearch/domain",
                  body={"DomainName": "desc-domain"})
        r = _req("GET", "2021-01-01/opensearch/domain/desc-domain")
        assert r.status_code == 200
        assert r.json()["DomainStatus"]["DomainName"] == "desc-domain"

    def test_describe_domain_not_found(self):
        r = _req("GET", "2021-01-01/opensearch/domain/nonexistent-xyz")
        assert r.status_code == 404

    def test_list_domain_names(self):
        _json_req("POST", "2021-01-01/opensearch/domain",
                  body={"DomainName": "list-dom-1"})
        _json_req("POST", "2021-01-01/opensearch/domain",
                  body={"DomainName": "list-dom-2"})
        r = _req("GET", "2021-01-01/opensearch/domain")
        assert r.status_code == 200
        names = [d["DomainName"] for d in r.json()["DomainNames"]]
        assert "list-dom-1" in names
        assert "list-dom-2" in names

    def test_delete_domain(self):
        _json_req("POST", "2021-01-01/opensearch/domain",
                  body={"DomainName": "del-domain"})
        r = _req("DELETE", "2021-01-01/opensearch/domain/del-domain")
        assert r.status_code == 200
        assert r.json()["DomainStatus"]["Deleted"] is True

    def test_add_and_list_tags(self):
        _json_req("POST", "2021-01-01/opensearch/domain",
                  body={"DomainName": "tag-domain"})
        arn = _req("GET", "2021-01-01/opensearch/domain/tag-domain").json()["DomainStatus"]["ARN"]
        _json_req("POST", "2021-01-01/opensearch/tags",
                  body={"ARN": arn, "TagList": [{"Key": "env", "Value": "test"}]})
        r = _req("GET", "2021-01-01/opensearch/tags", params={"ARN": arn})
        assert r.status_code == 200
        tags = {t["Key"]: t["Value"] for t in r.json()["TagList"]}
        assert tags.get("env") == "test"

    def test_remove_tags(self):
        _json_req("POST", "2021-01-01/opensearch/domain",
                  body={"DomainName": "rmtag-domain"})
        arn = _req("GET", "2021-01-01/opensearch/domain/rmtag-domain").json()["DomainStatus"]["ARN"]
        _json_req("POST", "2021-01-01/opensearch/tags",
                  body={"ARN": arn, "TagList": [{"Key": "k1", "Value": "v1"}, {"Key": "k2", "Value": "v2"}]})
        _json_req("POST", "2021-01-01/opensearch/tags/removal",
                  body={"ARN": arn, "TagKeys": ["k1"]})
        r = _req("GET", "2021-01-01/opensearch/tags", params={"ARN": arn})
        keys = [t["Key"] for t in r.json()["TagList"]]
        assert "k1" not in keys
        assert "k2" in keys

    def test_action_param_create_domain(self):
        r = _json_req("POST", "/",
                      body={"Action": "CreateDomain", "DomainName": "action-domain"})
        assert r.status_code == 200
        assert r.json()["DomainStatus"]["DomainName"] == "action-domain"

    def test_action_param_list_domain_names(self):
        r = _req("GET", "/", params={"Action": "ListDomainNames"})
        assert r.status_code == 200
        assert "DomainNames" in r.json()




class TestOpenSearchIndexManagement:

    def test_create_and_get_index(self):
        r = _json_req("PUT", "/my-index")
        assert r.status_code == 200
        data = r.json()
        assert data["acknowledged"] is True
        assert data["index"] == "my-index"

        r2 = _req("GET", "/my-index")
        assert r2.status_code == 200
        assert "my-index" in r2.json()

    def test_create_duplicate_index_fails(self):
        _json_req("PUT", "/dup-idx")
        r = _json_req("PUT", "/dup-idx")
        assert r.status_code == 400

    def test_delete_index(self):
        _json_req("PUT", "/del-idx")
        r = _req("DELETE", "/del-idx")
        assert r.status_code == 200
        assert r.json()["acknowledged"] is True

    def test_head_index_exists(self):
        _json_req("PUT", "/head-idx")
        r = _req("HEAD", "/head-idx")
        assert r.status_code == 200

    def test_head_index_missing(self):
        r = _req("HEAD", "/nonexistent-idx-xyz")
        assert r.status_code == 404

    def test_put_and_get_mapping(self):
        _json_req("PUT", "/map-idx")
        mapping = {"properties": {"title": {"type": "text"}, "age": {"type": "integer"}}}
        r = _json_req("PUT", "/map-idx/_mapping", body=mapping)
        assert r.status_code == 200

        r2 = _req("GET", "/map-idx/_mapping")
        assert r2.status_code == 200
        assert "map-idx" in r2.json()

    def test_put_and_get_settings(self):
        _json_req("PUT", "/set-idx")
        r = _json_req("PUT", "/set-idx/_settings",
                      body={"index": {"number_of_replicas": "1"}})
        assert r.status_code == 200
        r2 = _req("GET", "/set-idx/_settings")
        assert r2.status_code == 200

    def test_alias_add_and_get(self):
        _json_req("PUT", "/alias-idx")
        r = _json_req("PUT", "/alias-idx/_alias/my-alias")
        assert r.status_code == 200
        r2 = _req("GET", "/alias-idx/_alias")
        assert "my-alias" in r2.json().get("alias-idx", {}).get("aliases", {})

    def test_global_aliases(self):
        _json_req("PUT", "/galias-idx")
        _json_req("PUT", "/galias-idx/_alias/global-alias")
        r = _req("GET", "/_aliases")
        assert r.status_code == 200
        assert "galias-idx" in r.json()

    def test_update_aliases_action(self):
        _json_req("PUT", "/ua-idx")
        r = _json_req("POST", "/_aliases",
                      body={"actions": [{"add": {"index": "ua-idx", "alias": "ua-alias"}}]})
        assert r.status_code == 200
        r2 = _req("GET", "/_aliases")
        assert "ua-alias" in r2.json().get("ua-idx", {}).get("aliases", {})


class TestOpenSearchDocumentCRUD:

    def test_index_and_get_doc(self):
        _json_req("PUT", "/docs-idx")
        r = _json_req("PUT", "/docs-idx/_doc/1", body={"title": "hello", "value": 42})
        assert r.status_code == 201
        assert r.json()["result"] == "created"

        r2 = _req("GET", "/docs-idx/_doc/1")
        assert r2.status_code == 200
        data = r2.json()
        assert data["found"] is True
        assert data["_source"]["title"] == "hello"
        assert data["_source"]["value"] == 42

    def test_update_doc(self):
        _json_req("PUT", "/upd-idx")
        _json_req("PUT", "/upd-idx/_doc/1", body={"field": "original"})
        r = _json_req("POST", "/upd-idx/_update/1", body={"doc": {"field": "updated"}})
        assert r.status_code == 200
        r2 = _req("GET", "/upd-idx/_doc/1")
        assert r2.json()["_source"]["field"] == "updated"

    def test_delete_doc(self):
        _json_req("PUT", "/deldoc-idx")
        _json_req("PUT", "/deldoc-idx/_doc/1", body={"x": 1})
        r = _req("DELETE", "/deldoc-idx/_doc/1")
        assert r.status_code == 200
        assert r.json()["result"] == "deleted"

        r2 = _req("GET", "/deldoc-idx/_doc/1")
        assert r2.status_code == 404
        assert r2.json()["found"] is False

    def test_get_missing_doc(self):
        _json_req("PUT", "/miss-idx")
        r = _req("GET", "/miss-idx/_doc/does-not-exist")
        assert r.status_code == 404
        assert r.json()["found"] is False

    def test_head_doc_exists(self):
        _json_req("PUT", "/hd-idx")
        _json_req("PUT", "/hd-idx/_doc/1", body={"k": "v"})
        r = _req("HEAD", "/hd-idx/_doc/1")
        assert r.status_code == 200

    def test_head_doc_missing(self):
        _json_req("PUT", "/hdm-idx")
        r = _req("HEAD", "/hdm-idx/_doc/nothere")
        assert r.status_code == 404

    def test_post_doc_auto_id(self):
        _json_req("PUT", "/auto-idx")
        r = _json_req("POST", "/auto-idx/_doc", body={"msg": "auto"})
        assert r.status_code == 201
        doc_id = r.json()["_id"]
        assert doc_id  # non-empty auto-generated ID

    def test_version_increments_on_update(self):
        _json_req("PUT", "/ver-idx")
        _json_req("PUT", "/ver-idx/_doc/1", body={"v": 1})
        r = _json_req("PUT", "/ver-idx/_doc/1", body={"v": 2})
        assert r.json()["_version"] == 2
        assert r.json()["result"] == "updated"


class TestOpenSearchSearch:

    def _seed_index(self, index_name, docs):
        _json_req("PUT", f"/{index_name}")
        for i, doc in enumerate(docs):
            _json_req("PUT", f"/{index_name}/_doc/{i+1}", body=doc)

    def test_match_all(self):
        self._seed_index("srch-all", [{"n": "a"}, {"n": "b"}, {"n": "c"}])
        r = _json_req("POST", "/srch-all/_search", body={"query": {"match_all": {}}})
        assert r.status_code == 200
        hits = r.json()["hits"]
        assert hits["total"]["value"] == 3

    def test_term_query(self):
        self._seed_index("srch-term", [{"status": "active"}, {"status": "inactive"}, {"status": "active"}])
        r = _json_req("POST", "/srch-term/_search",
                      body={"query": {"term": {"status": "active"}}})
        assert r.json()["hits"]["total"]["value"] == 2

    def test_match_query(self):
        self._seed_index("srch-match",
                         [{"text": "hello world"}, {"text": "goodbye moon"}, {"text": "hello mars"}])
        r = _json_req("POST", "/srch-match/_search",
                      body={"query": {"match": {"text": "hello"}}})
        assert r.json()["hits"]["total"]["value"] == 2

    def test_bool_must_query(self):
        self._seed_index("srch-bool",
                         [{"cat": "a", "val": 1}, {"cat": "b", "val": 1}, {"cat": "a", "val": 2}])
        r = _json_req("POST", "/srch-bool/_search", body={
            "query": {"bool": {"must": [{"term": {"cat": "a"}}, {"term": {"val": 1}}]}}
        })
        assert r.json()["hits"]["total"]["value"] == 1

    def test_bool_should_query(self):
        self._seed_index("srch-should",
                         [{"cat": "a"}, {"cat": "b"}, {"cat": "c"}])
        r = _json_req("POST", "/srch-should/_search", body={
            "query": {"bool": {"should": [{"term": {"cat": "a"}}, {"term": {"cat": "b"}}]}}
        })
        assert r.json()["hits"]["total"]["value"] == 2

    def test_bool_must_not(self):
        self._seed_index("srch-must-not",
                         [{"status": "active"}, {"status": "inactive"}])
        r = _json_req("POST", "/srch-must-not/_search", body={
            "query": {"bool": {"must_not": [{"term": {"status": "inactive"}}]}}
        })
        assert r.json()["hits"]["total"]["value"] == 1

    def test_range_query(self):
        self._seed_index("srch-range",
                         [{"price": 10}, {"price": 50}, {"price": 100}])
        r = _json_req("POST", "/srch-range/_search",
                      body={"query": {"range": {"price": {"gte": 20, "lte": 80}}}})
        assert r.json()["hits"]["total"]["value"] == 1

    def test_exists_query(self):
        self._seed_index("srch-exists",
                         [{"field_a": "val"}, {"field_b": "other"}])
        r = _json_req("POST", "/srch-exists/_search",
                      body={"query": {"exists": {"field": "field_a"}}})
        assert r.json()["hits"]["total"]["value"] == 1

    def test_prefix_query(self):
        self._seed_index("srch-prefix",
                         [{"name": "alpha"}, {"name": "beta"}, {"name": "alphabet"}])
        r = _json_req("POST", "/srch-prefix/_search",
                      body={"query": {"prefix": {"name": "alpha"}}})
        assert r.json()["hits"]["total"]["value"] == 2

    def test_terms_query(self):
        self._seed_index("srch-terms",
                         [{"color": "red"}, {"color": "blue"}, {"color": "green"}])
        r = _json_req("POST", "/srch-terms/_search",
                      body={"query": {"terms": {"color": ["red", "green"]}}})
        assert r.json()["hits"]["total"]["value"] == 2

    def test_from_and_size_pagination(self):
        self._seed_index("srch-page",
                         [{"n": i} for i in range(10)])
        r = _json_req("POST", "/srch-page/_search",
                      body={"query": {"match_all": {}}, "from": 3, "size": 4})
        hits = r.json()["hits"]["hits"]
        assert len(hits) == 4

    def test_source_filtering(self):
        self._seed_index("srch-src",
                         [{"a": 1, "b": 2, "c": 3}])
        r = _json_req("POST", "/srch-src/_search",
                      body={"query": {"match_all": {}}, "_source": ["a", "b"]})
        source = r.json()["hits"]["hits"][0]["_source"]
        assert "a" in source and "b" in source and "c" not in source

    def test_sort(self):
        self._seed_index("srch-sort",
                         [{"score": 3}, {"score": 1}, {"score": 2}])
        r = _json_req("POST", "/srch-sort/_search",
                      body={"query": {"match_all": {}}, "sort": [{"score": "asc"}]})
        scores = [h["_source"]["score"] for h in r.json()["hits"]["hits"]]
        assert scores == [1, 2, 3]

    def test_cross_index_search(self):
        self._seed_index("ci-idx-1", [{"tag": "foo"}])
        self._seed_index("ci-idx-2", [{"tag": "foo"}, {"tag": "bar"}])
        r = _json_req("POST", "/_search", body={"query": {"term": {"tag": "foo"}}})
        assert r.json()["hits"]["total"]["value"] >= 2

    def test_wildcard_query(self):
        self._seed_index("srch-wild",
                         [{"name": "alpha"}, {"name": "beta"}, {"name": "alphabeta"}])
        r = _json_req("POST", "/srch-wild/_search",
                      body={"query": {"wildcard": {"name": "alph*"}}})
        assert r.json()["hits"]["total"]["value"] == 2

    def test_match_phrase_query(self):
        self._seed_index("srch-phrase",
                         [{"text": "quick brown fox"}, {"text": "quick fox"}, {"text": "slow brown fox"}])
        r = _json_req("POST", "/srch-phrase/_search",
                      body={"query": {"match_phrase": {"text": "quick brown"}}})
        assert r.json()["hits"]["total"]["value"] == 1


class TestOpenSearchCount:

    def test_count_all(self):
        _json_req("PUT", "/cnt-idx")
        for i in range(5):
            _json_req("PUT", f"/cnt-idx/_doc/{i}", body={"x": i})
        r = _json_req("POST", "/cnt-idx/_count", body={})
        assert r.json()["count"] == 5

    def test_count_with_query(self):
        _json_req("PUT", "/cnt2-idx")
        _json_req("PUT", "/cnt2-idx/_doc/1", body={"active": True})
        _json_req("PUT", "/cnt2-idx/_doc/2", body={"active": False})
        _json_req("PUT", "/cnt2-idx/_doc/3", body={"active": True})
        r = _json_req("POST", "/cnt2-idx/_count",
                      body={"query": {"term": {"active": True}}})
        assert r.json()["count"] == 2


class TestOpenSearchBulk:

    def test_bulk_index(self):
        _json_req("PUT", "/bulk-idx")
        ndjson = (
            '{"index": {"_index": "bulk-idx", "_id": "b1"}}\n'
            '{"name": "doc one"}\n'
            '{"index": {"_index": "bulk-idx", "_id": "b2"}}\n'
            '{"name": "doc two"}\n'
        )
        r = requests.post(f"{BASE}/_bulk",
                          data=ndjson,
                          headers={"Content-Type": "application/x-ndjson"})
        assert r.status_code == 200
        assert r.json()["errors"] is False
        assert len(r.json()["items"]) == 2

    def test_bulk_delete(self):
        _json_req("PUT", "/bdel-idx")
        _json_req("PUT", "/bdel-idx/_doc/d1", body={"k": "v"})
        ndjson = '{"delete": {"_index": "bdel-idx", "_id": "d1"}}\n'
        r = requests.post(f"{BASE}/_bulk",
                          data=ndjson,
                          headers={"Content-Type": "application/x-ndjson"})
        assert r.status_code == 200
        assert r.json()["items"][0]["delete"]["result"] == "deleted"
        r2 = _req("GET", "/bdel-idx/_doc/d1")
        assert r2.json()["found"] is False

    def test_bulk_update(self):
        _json_req("PUT", "/bupd-idx")
        _json_req("PUT", "/bupd-idx/_doc/u1", body={"val": 1})
        ndjson = (
            '{"update": {"_index": "bupd-idx", "_id": "u1"}}\n'
            '{"doc": {"val": 99}}\n'
        )
        r = requests.post(f"{BASE}/_bulk",
                          data=ndjson,
                          headers={"Content-Type": "application/x-ndjson"})
        assert r.status_code == 200
        r2 = _req("GET", "/bupd-idx/_doc/u1")
        assert r2.json()["_source"]["val"] == 99

    def test_bulk_mixed(self):
        _json_req("PUT", "/bmix-idx")
        ndjson = (
            '{"index": {"_index": "bmix-idx", "_id": "m1"}}\n'
            '{"v": 1}\n'
            '{"index": {"_index": "bmix-idx", "_id": "m2"}}\n'
            '{"v": 2}\n'
            '{"delete": {"_index": "bmix-idx", "_id": "m1"}}\n'
        )
        requests.post(f"{BASE}/_bulk", data=ndjson,
                      headers={"Content-Type": "application/x-ndjson"})
        r = _req("GET", "/bmix-idx/_doc/m1")
        assert r.json()["found"] is False
        r2 = _req("GET", "/bmix-idx/_doc/m2")
        assert r2.json()["found"] is True


class TestOpenSearchAggregations:

    def _seed(self, name, docs):
        _json_req("PUT", f"/{name}")
        for i, d in enumerate(docs):
            _json_req("PUT", f"/{name}/_doc/{i}", body=d)

    def test_terms_agg(self):
        self._seed("agg-terms", [{"cat": "a"}, {"cat": "b"}, {"cat": "a"}])
        r = _json_req("POST", "/agg-terms/_search", body={
            "query": {"match_all": {}},
            "aggs": {"cats": {"terms": {"field": "cat"}}}
        })
        buckets = {b["key"]: b["doc_count"] for b in r.json()["aggregations"]["cats"]["buckets"]}
        assert buckets["a"] == 2
        assert buckets["b"] == 1

    def test_sum_agg(self):
        self._seed("agg-sum", [{"price": 10}, {"price": 20}, {"price": 30}])
        r = _json_req("POST", "/agg-sum/_search", body={
            "aggs": {"total": {"sum": {"field": "price"}}}
        })
        assert r.json()["aggregations"]["total"]["value"] == 60

    def test_avg_agg(self):
        self._seed("agg-avg", [{"score": 10}, {"score": 20}, {"score": 30}])
        r = _json_req("POST", "/agg-avg/_search", body={
            "aggs": {"avg_score": {"avg": {"field": "score"}}}
        })
        assert r.json()["aggregations"]["avg_score"]["value"] == 20.0

    def test_min_max_agg(self):
        self._seed("agg-mm", [{"v": 5}, {"v": 15}, {"v": 3}])
        r = _json_req("POST", "/agg-mm/_search", body={
            "aggs": {"lo": {"min": {"field": "v"}}, "hi": {"max": {"field": "v"}}}
        })
        aggs = r.json()["aggregations"]
        assert aggs["lo"]["value"] == 3
        assert aggs["hi"]["value"] == 15

    def test_value_count_agg(self):
        self._seed("agg-vc", [{"x": 1}, {"x": 2}, {}])
        r = _json_req("POST", "/agg-vc/_search", body={
            "aggs": {"xcount": {"value_count": {"field": "x"}}}
        })
        assert r.json()["aggregations"]["xcount"]["value"] == 2

    def test_cardinality_agg(self):
        self._seed("agg-card", [{"col": "a"}, {"col": "b"}, {"col": "a"}, {"col": "c"}])
        r = _json_req("POST", "/agg-card/_search", body={
            "aggs": {"unique": {"cardinality": {"field": "col"}}}
        })
        assert r.json()["aggregations"]["unique"]["value"] == 3


class TestOpenSearchDeleteAndUpdateByQuery:

    def test_delete_by_query(self):
        _json_req("PUT", "/dbq-idx")
        for i in range(5):
            _json_req("PUT", f"/dbq-idx/_doc/{i}", body={"keep": i < 3})
        r = _json_req("POST", "/dbq-idx/_delete_by_query",
                      body={"query": {"term": {"keep": False}}})
        assert r.json()["deleted"] == 2
        r2 = _json_req("POST", "/dbq-idx/_count", body={})
        assert r2.json()["count"] == 3

    def test_update_by_query(self):
        _json_req("PUT", "/ubq-idx")
        _json_req("PUT", "/ubq-idx/_doc/1", body={"status": "old", "val": 0})
        _json_req("PUT", "/ubq-idx/_doc/2", body={"status": "new", "val": 0})
        r = _json_req("POST", "/ubq-idx/_update_by_query", body={
            "query": {"term": {"status": "old"}},
            "script": {"source": "ctx._source.val = params.newval", "params": {"newval": 99}}
        })
        assert r.json()["updated"] == 1
        r2 = _req("GET", "/ubq-idx/_doc/1")
        assert r2.json()["_source"]["val"] == 99


class TestOpenSearchMget:

    def test_mget(self):
        _json_req("PUT", "/mget-idx")
        _json_req("PUT", "/mget-idx/_doc/1", body={"x": 1})
        _json_req("PUT", "/mget-idx/_doc/2", body={"x": 2})
        r = _json_req("POST", "/mget-idx/_mget",
                      body={"docs": [{"_id": "1"}, {"_id": "2"}, {"_id": "999"}]})
        assert r.status_code == 200
        docs = r.json()["docs"]
        assert docs[0]["found"] is True
        assert docs[1]["found"] is True
        assert docs[2]["found"] is False


class TestOpenSearchScrollAPI:

    def test_scroll_basic(self):
        _json_req("PUT", "/scroll-idx")
        for i in range(15):
            _json_req("PUT", f"/scroll-idx/_doc/{i}", body={"n": i})

        # Initial search with scroll
        r = _json_req("POST", "/scroll-idx/_search",
                      body={"query": {"match_all": {}}, "size": 5},
                      params={"scroll": "1m"})
        data = r.json()
        assert len(data["hits"]["hits"]) == 5
        assert "_scroll_id" in data
        scroll_id = data["_scroll_id"]

        # Fetch next page
        r2 = _json_req("POST", "/_search/scroll",
                       body={"scroll": "1m", "scroll_id": scroll_id})
        assert len(r2.json()["hits"]["hits"]) == 5

    def test_scroll_delete(self):
        _json_req("PUT", "/sdel-idx")
        _json_req("PUT", "/sdel-idx/_doc/1", body={"k": "v"})
        r = _json_req("POST", "/sdel-idx/_search",
                      body={"query": {"match_all": {}}, "size": 1},
                      params={"scroll": "1m"})
        sid = r.json()["_scroll_id"]
        r2 = requests.delete(f"{BASE}/_search/scroll",
                             json={"scroll_id": sid},
                             headers={"Content-Type": "application/json"})
        assert r2.status_code == 200
        assert r2.json()["succeeded"] is True


class TestOpenSearchClusterEndpoints:

    def test_root_info(self):
        r = _req("GET", "/")
        assert r.status_code == 200
        data = r.json()
        assert "version" in data
        assert data["version"]["distribution"] == "opensearch"

    def test_cluster_health(self):
        r = _req("GET", "/_cluster/health")
        assert r.status_code == 200
        assert r.json()["status"] == "green"

    def test_cluster_stats(self):
        r = _req("GET", "/_cluster/stats")
        assert r.status_code == 200
        assert "indices" in r.json()

    def test_cat_indices(self):
        _json_req("PUT", "/cat-test-idx")
        _json_req("PUT", "/cat-test-idx/_doc/1", body={"k": "v"})
        r = _req("GET", "/_cat/indices")
        assert r.status_code == 200

    def test_cat_health(self):
        r = _req("GET", "/_cat/health")
        assert r.status_code == 200

    def test_cat_nodes(self):
        r = _req("GET", "/_cat/nodes")
        assert r.status_code == 200

    def test_nodes_info(self):
        r = _req("GET", "/_nodes")
        assert r.status_code == 200
        assert "nodes" in r.json()


class TestOpenSearchIndexTemplate:

    def test_create_and_get_template(self):
        tmpl = {"index_patterns": ["logs-*"], "template": {"settings": {"number_of_shards": 1}}}
        r = _json_req("PUT", "/_index_template/logs-tmpl", body=tmpl)
        assert r.status_code == 200

        r2 = _req("GET", "/_index_template/logs-tmpl")
        assert r2.status_code == 200
        templates = r2.json()["index_templates"]
        assert any(t["name"] == "logs-tmpl" for t in templates)

    def test_list_templates(self):
        _json_req("PUT", "/_index_template/list-tmpl",
                  body={"index_patterns": ["list-*"]})
        r = _req("GET", "/_index_template")
        assert r.status_code == 200
        names = [t["name"] for t in r.json()["index_templates"]]
        assert "list-tmpl" in names

    def test_delete_template(self):
        _json_req("PUT", "/_index_template/del-tmpl",
                  body={"index_patterns": ["del-*"]})
        r = _req("DELETE", "/_index_template/del-tmpl")
        assert r.status_code == 200
        r2 = _req("GET", "/_index_template/del-tmpl")
        assert r2.status_code == 404
