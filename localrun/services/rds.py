"""RDS service emulator (stub)."""
import json, logging, time, uuid
from flask import Request, Response
from localrun.config import get_config
from localrun.utils import error_response, new_request_id

logger = logging.getLogger("localrun.rds")

class RDSService:
    def __init__(self):
        self.instances = {}  # id -> instance dict
        self.clusters = {}   # id -> cluster dict

    def handle(self, req: Request, path: str) -> Response:
        action = req.args.get("Action") or req.form.get("Action", "")
        handlers = {
            "CreateDBInstance": self._create_instance,
            "DeleteDBInstance": self._delete_instance,
            "DescribeDBInstances": self._describe_instances,
            "CreateDBCluster": self._create_cluster,
            "DeleteDBCluster": self._delete_cluster,
            "DescribeDBClusters": self._describe_clusters,
        }
        h = handlers.get(action)
        if not h:
            return error_response("InvalidAction", f"Action {action} not valid", 400)
        return h(req)

    def _p(self, req):
        from urllib.parse import parse_qs
        params = dict(req.args)
        if req.content_type and "form" in req.content_type:
            params.update(req.form.to_dict())
        if not params.get("Action"):
            for k, v in parse_qs(req.get_data(as_text=True)).items():
                params[k] = v[0] if len(v) == 1 else v
        return params

    def _xml(self, action, content):
        body = f'<?xml version="1.0"?>\n<{action}Response xmlns="http://rds.amazonaws.com/doc/2014-10-31/">\n  <{action}Result>\n{content}\n  </{action}Result>\n  <ResponseMetadata><RequestId>{new_request_id()}</RequestId></ResponseMetadata>\n</{action}Response>'
        return Response(body, 200, content_type="application/xml")

    def _create_instance(self, req):
        p = self._p(req)
        dbid = p.get("DBInstanceIdentifier", "")
        if not dbid: return error_response("InvalidParameterValue", "DBInstanceIdentifier required", 400)
        c = get_config()
        arn = f"arn:aws:rds:{c.region}:{c.account_id}:db:{dbid}"
        self.instances[dbid] = {
            "DBInstanceIdentifier": dbid, "DBInstanceArn": arn,
            "DBInstanceStatus": "available",
            "Engine": p.get("Engine", "mysql"),
            "DBInstanceClass": p.get("DBInstanceClass", "db.t3.micro"),
            "Endpoint": {"Address": f"{dbid}.{c.account_id}.{c.region}.rds.localhost", "Port": 3306},
        }
        logger.info("Created DB instance: %s", dbid)
        return self._xml("CreateDBInstance", f"    <DBInstance><DBInstanceIdentifier>{dbid}</DBInstanceIdentifier><DBInstanceStatus>available</DBInstanceStatus><DBInstanceArn>{arn}</DBInstanceArn></DBInstance>")

    def _delete_instance(self, req):
        p = self._p(req)
        dbid = p.get("DBInstanceIdentifier", "")
        self.instances.pop(dbid, None)
        return self._xml("DeleteDBInstance", "")

    def _describe_instances(self, req):
        p = self._p(req)
        dbid = p.get("DBInstanceIdentifier")
        instances = [self.instances[dbid]] if dbid and dbid in self.instances else list(self.instances.values())
        members = ""
        for inst in instances:
            members += f"""      <DBInstance>
        <DBInstanceIdentifier>{inst['DBInstanceIdentifier']}</DBInstanceIdentifier>
        <DBInstanceStatus>{inst['DBInstanceStatus']}</DBInstanceStatus>
        <DBInstanceArn>{inst['DBInstanceArn']}</DBInstanceArn>
        <Engine>{inst['Engine']}</Engine>
      </DBInstance>\n"""
        return self._xml("DescribeDBInstances", f"    <DBInstances>\n{members}    </DBInstances>")

    def _create_cluster(self, req):
        p = self._p(req)
        cid = p.get("DBClusterIdentifier", "")
        if not cid: return error_response("InvalidParameterValue", "DBClusterIdentifier required", 400)
        c = get_config()
        arn = f"arn:aws:rds:{c.region}:{c.account_id}:cluster:{cid}"
        self.clusters[cid] = {
            "DBClusterIdentifier": cid, "DBClusterArn": arn,
            "Status": "available", "Engine": p.get("Engine", "aurora-mysql"),
        }
        logger.info("Created DB cluster: %s", cid)
        return self._xml("CreateDBCluster", f"    <DBCluster><DBClusterIdentifier>{cid}</DBClusterIdentifier><Status>available</Status><DBClusterArn>{arn}</DBClusterArn></DBCluster>")

    def _delete_cluster(self, req):
        p = self._p(req)
        cid = p.get("DBClusterIdentifier", "")
        self.clusters.pop(cid, None)
        return self._xml("DeleteDBCluster", "")

    def _describe_clusters(self, req):
        p = self._p(req)
        cid = p.get("DBClusterIdentifier")
        clusters = [self.clusters[cid]] if cid and cid in self.clusters else list(self.clusters.values())
        members = ""
        for cl in clusters:
            members += f"""      <DBCluster>
        <DBClusterIdentifier>{cl['DBClusterIdentifier']}</DBClusterIdentifier>
        <Status>{cl['Status']}</Status>
        <DBClusterArn>{cl['DBClusterArn']}</DBClusterArn>
      </DBCluster>\n"""
        return self._xml("DescribeDBClusters", f"    <DBClusters>\n{members}    </DBClusters>")

    def reset(self):
        self.instances = {}
        self.clusters = {}
