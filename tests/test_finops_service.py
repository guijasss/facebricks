import unittest

from src.app.config import AppConfig
from src.app.finops_service import FinOpsService
from src.app.storage import InMemoryStorage
from src.entities import ClusterSpec, Job, Run


class FinOpsServiceTest(unittest.TestCase):
    def test_dashboard_uses_saved_cluster_pricing_config(self) -> None:
        config = AppConfig(
            database_url="postgresql://facebrick:facebrick@db:5432/facebrick",
            pricing_data={},
        )
        storage = InMemoryStorage()
        storage.save_jobs(
            [
                Job(
                    job_id=1,
                    name="bronze-refresh",
                    tags={"pipeline": "ingestion", "tables": "bronze.events"},
                ),
                Job(
                    job_id=2,
                    name="gold-aggregate",
                    tags={"pipeline": "serving", "tables": "gold.orders"},
                ),
            ]
        )
        storage.save_runs(
            [
                Run(
                    run_id=100,
                    job_id=1,
                    start_time=1_700_000_000_000,
                    duration_ms=3_600_000,
                    cluster_spec=ClusterSpec(node_type_id="m5d.large", num_workers=1),
                ),
                Run(
                    run_id=101,
                    job_id=2,
                    start_time=1_700_050_000_000,
                    duration_ms=7_200_000,
                    cluster_spec=ClusterSpec(node_type_id="m5d.large", num_workers=2),
                ),
            ]
        )
        storage.write_metadata(
            "cluster_pricing_config",
            (
                '{"cluster_node_types":["m5d.large"],"entries":{"m5d.large":{"dbus_per_hour":2.0,"plan":"premium"}},"last_refreshed_at":"2026-04-08T00:00:00+00:00"}'
            ),
        )

        service = FinOpsService(config=config, storage=storage)
        dashboard = service.get_dashboard(window_days=3650)

        self.assertEqual(dashboard["summary"]["currency"], "USD")
        self.assertAlmostEqual(dashboard["summary"]["total_cost"], 2.4)
        self.assertEqual(dashboard["summary"]["pipeline_count"], 2)
        self.assertEqual(dashboard["summary"]["table_count"], 2)
        self.assertEqual(dashboard["summary"]["most_expensive_pipeline"], "serving")
        self.assertEqual(dashboard["summary"]["most_expensive_table"], "gold.orders")
        self.assertEqual(dashboard["coverage"]["costed_runs"], 2)
        self.assertEqual(dashboard["top_jobs"][0]["job_name"], "gold-aggregate")
        self.assertEqual(dashboard["top_pipelines"][0]["label"], "serving")
        self.assertEqual(dashboard["top_tables"][0]["label"], "gold.orders")
        self.assertTrue(
            any(insight["subject_type"] == "table" for insight in dashboard["insights"])
        )
        self.assertEqual(len(dashboard["recent_runs"]), 2)

    def test_cluster_pricing_config_round_trip(self) -> None:
        config = AppConfig(
            database_url="postgresql://facebrick:facebrick@db:5432/facebrick",
            pricing_data={},
        )
        storage = InMemoryStorage()
        storage.write_metadata(
            "cluster_pricing_config",
            '{"cluster_node_types":["m5d.large","i3.xlarge"],"entries":{},"last_refreshed_at":"2026-04-08T00:00:00+00:00"}',
        )
        service = FinOpsService(config=config, storage=storage)

        payload = service.save_cluster_pricing_config(
            [
                {
                    "node_type_id": "m5d.large",
                    "dbus_per_hour": 2.0,
                    "plan": "premium",
                },
                {
                    "node_type_id": "i3.xlarge",
                    "dbus_per_hour": 3.0,
                    "plan": "enterprise",
                },
            ]
        )

        self.assertEqual(payload["cluster_node_types"], ["m5d.large", "i3.xlarge"])
        self.assertEqual(payload["entries"][0]["plan"], "premium")
        self.assertAlmostEqual(payload["entries"][0]["jobs_rate_per_hour"], 0.3)
        self.assertEqual(payload["entries"][1]["plan"], "enterprise")
        self.assertAlmostEqual(payload["entries"][1]["all_purpose_rate_per_hour"], 1.95)


if __name__ == "__main__":
    unittest.main()
