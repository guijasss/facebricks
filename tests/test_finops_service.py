import json
import unittest
from pathlib import Path

from src.app.config import AppConfig
from src.app.finops_service import FinOpsService
from src.app.storage import InMemoryStorage
from src.entities import ClusterSpec, Job, Run


class FinOpsServiceTest(unittest.TestCase):
    def test_dashboard_aggregates_stored_jobs_and_runs(self) -> None:
        pricing_file = Path("tests/pricing.test.json")
        pricing_file.write_text(
            json.dumps(
                {
                    "currency": "USD",
                    "rates": [
                        {
                            "node_type_id": "m5d.large",
                            "dbu_rate_per_hour": 1.0,
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        self.addCleanup(lambda: pricing_file.unlink(missing_ok=True))

        config = AppConfig(
            database_url="postgresql://facebrick:facebrick@db:5432/facebrick",
            pricing_file=pricing_file,
        )
        storage = InMemoryStorage()
        storage.save_jobs(
            [
                Job(job_id=1, name="bronze-refresh"),
                Job(job_id=2, name="gold-aggregate"),
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

        service = FinOpsService(config=config, storage=storage)
        dashboard = service.get_dashboard(window_days=3650)

        self.assertEqual(dashboard["summary"]["currency"], "USD")
        self.assertAlmostEqual(dashboard["summary"]["total_cost"], 8.0)
        self.assertEqual(dashboard["coverage"]["costed_runs"], 2)
        self.assertEqual(dashboard["top_jobs"][0]["job_name"], "gold-aggregate")
        self.assertEqual(len(dashboard["recent_runs"]), 2)


if __name__ == "__main__":
    unittest.main()
