"""RDS service tests — instances and clusters."""
import pytest


class TestRDSInstances:
    def test_create_db_instance(self, rds_client):
        resp = rds_client.create_db_instance(
            DBInstanceIdentifier="mydb",
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password",
        )
        assert resp["DBInstance"]["DBInstanceIdentifier"] == "mydb"
        assert resp["DBInstance"]["DBInstanceStatus"] == "available"

    def test_describe_db_instances_all(self, rds_client):
        rds_client.create_db_instance(
            DBInstanceIdentifier="desc-db",
            DBInstanceClass="db.t3.micro",
            Engine="postgres",
            MasterUsername="u",
            MasterUserPassword="p",
        )
        instances = rds_client.describe_db_instances()["DBInstances"]
        ids = [i["DBInstanceIdentifier"] for i in instances]
        assert "desc-db" in ids

    def test_describe_db_instances_by_id(self, rds_client):
        rds_client.create_db_instance(
            DBInstanceIdentifier="single-db",
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="u",
            MasterUserPassword="p",
        )
        resp = rds_client.describe_db_instances(DBInstanceIdentifier="single-db")
        assert len(resp["DBInstances"]) == 1
        assert resp["DBInstances"][0]["DBInstanceIdentifier"] == "single-db"

    def test_instance_engine_stored(self, rds_client):
        rds_client.create_db_instance(
            DBInstanceIdentifier="eng-db",
            DBInstanceClass="db.t3.micro",
            Engine="postgres",
            MasterUsername="u",
            MasterUserPassword="p",
        )
        inst = rds_client.describe_db_instances(DBInstanceIdentifier="eng-db")["DBInstances"][0]
        assert inst["Engine"] == "postgres"

    def test_delete_db_instance(self, rds_client):
        rds_client.create_db_instance(
            DBInstanceIdentifier="del-db",
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="u",
            MasterUserPassword="p",
        )
        rds_client.delete_db_instance(
            DBInstanceIdentifier="del-db",
            SkipFinalSnapshot=True,
        )
        instances = rds_client.describe_db_instances()["DBInstances"]
        ids = [i["DBInstanceIdentifier"] for i in instances]
        assert "del-db" not in ids


class TestRDSClusters:
    def test_create_db_cluster(self, rds_client):
        resp = rds_client.create_db_cluster(
            DBClusterIdentifier="my-cluster",
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password",
        )
        assert resp["DBCluster"]["DBClusterIdentifier"] == "my-cluster"

    def test_describe_db_clusters_all(self, rds_client):
        rds_client.create_db_cluster(
            DBClusterIdentifier="desc-cluster",
            Engine="aurora-mysql",
            MasterUsername="u",
            MasterUserPassword="p",
        )
        clusters = rds_client.describe_db_clusters()["DBClusters"]
        ids = [c["DBClusterIdentifier"] for c in clusters]
        assert "desc-cluster" in ids

    def test_describe_db_clusters_by_id(self, rds_client):
        rds_client.create_db_cluster(
            DBClusterIdentifier="single-cluster",
            Engine="aurora-postgresql",
            MasterUsername="u",
            MasterUserPassword="p",
        )
        resp = rds_client.describe_db_clusters(DBClusterIdentifier="single-cluster")
        assert len(resp["DBClusters"]) == 1
        assert resp["DBClusters"][0]["DBClusterIdentifier"] == "single-cluster"

    def test_delete_db_cluster(self, rds_client):
        rds_client.create_db_cluster(
            DBClusterIdentifier="del-cluster",
            Engine="aurora-mysql",
            MasterUsername="u",
            MasterUserPassword="p",
        )
        rds_client.delete_db_cluster(
            DBClusterIdentifier="del-cluster",
            SkipFinalSnapshot=True,
        )
        clusters = rds_client.describe_db_clusters()["DBClusters"]
        ids = [c["DBClusterIdentifier"] for c in clusters]
        assert "del-cluster" not in ids

    def test_cluster_arn_format(self, rds_client):
        resp = rds_client.create_db_cluster(
            DBClusterIdentifier="arn-cluster",
            Engine="aurora-mysql",
            MasterUsername="u",
            MasterUserPassword="p",
        )
        arn = resp["DBCluster"]["DBClusterArn"]
        assert "arn:aws:rds:" in arn
        assert "arn-cluster" in arn
