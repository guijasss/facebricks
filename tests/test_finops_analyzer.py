import unittest

from src.entities import Autoscale, Cluster, ClusterInstance, ClusterSpec, Job, Run
from src.finops import FinOpsAnalyzer, FinOpsConfig, PricingRate


class FinOpsAnalyzerTest(unittest.TestCase):
    def test_estimate_run_cost_uses_run_cluster_spec(self) -> None:
        analyzer = FinOpsAnalyzer(
            pricing_rates=[PricingRate(node_type_id="m5d.large", dbu_rate_per_hour=0.5)]
        )
        run = Run(
            run_id=10,
            job_id=1,
            duration_ms=7_200_000,
            cluster_spec=ClusterSpec(node_type_id="m5d.large", num_workers=2),
        )

        result = analyzer.estimate_run_cost(run)

        self.assertIsNotNone(result)
        self.assertEqual(result.source, "run.cluster_spec")
        self.assertAlmostEqual(result.billable_nodes, 3.0)
        self.assertAlmostEqual(result.hourly_rate, 1.5)
        self.assertAlmostEqual(result.estimated_cost, 3.0)

    def test_estimate_run_cost_falls_back_to_cluster_lookup(self) -> None:
        analyzer = FinOpsAnalyzer(
            pricing_rates=[PricingRate(node_type_id="m5d.large", dbu_rate_per_hour=1.2)],
            clusters=[
                Cluster(
                    cluster_id="abc",
                    node_type_id="m5d.large",
                    num_workers=1,
                )
            ],
        )
        run = Run(
            run_id=11,
            job_id=2,
            duration_ms=1_800_000,
            cluster_instance=ClusterInstance(cluster_id="abc"),
        )

        result = analyzer.estimate_run_cost(run)

        self.assertIsNotNone(result)
        self.assertEqual(result.source, "cluster_lookup")
        self.assertAlmostEqual(result.billable_nodes, 2.0)
        self.assertAlmostEqual(result.estimated_cost, 1.2)

    def test_build_report_aggregates_job_costs_and_insights(self) -> None:
        analyzer = FinOpsAnalyzer(
            pricing_rates=[PricingRate(node_type_id="m5d.large", dbu_rate_per_hour=1.0)],
            config=FinOpsConfig(expensive_job_share_threshold=0.30, top_n_insights=2),
        )
        jobs = [
            Job(job_id=1, name="bronze-refresh"),
            Job(job_id=2, name="gold-aggregate"),
        ]
        runs = [
            Run(
                run_id=100,
                job_id=1,
                start_time=1_000,
                duration_ms=3_600_000,
                cluster_spec=ClusterSpec(node_type_id="m5d.large", num_workers=3),
            ),
            Run(
                run_id=101,
                job_id=1,
                start_time=2_000,
                duration_ms=1_800_000,
                cluster_spec=ClusterSpec(node_type_id="m5d.large", num_workers=1),
            ),
            Run(
                run_id=102,
                job_id=2,
                start_time=3_000,
                duration_ms=3_600_000,
                cluster_spec=ClusterSpec(node_type_id="m5d.large", num_workers=1),
            ),
        ]

        report = analyzer.build_report(jobs=jobs, runs=runs)

        self.assertAlmostEqual(report.total_cost, 7.0)
        self.assertEqual([summary.job_id for summary in report.job_summaries], [1, 2])
        self.assertAlmostEqual(report.job_summaries[0].total_cost, 5.0)
        self.assertAlmostEqual(report.job_summaries[0].cost_share, 5 / 7)
        self.assertAlmostEqual(report.job_summaries[0].avg_cost_per_run, 2.5)
        self.assertEqual(report.job_summaries[0].last_run_time, 2_000)
        self.assertTrue(any(insight.kind == "dominant_cost_share" for insight in report.insights))

    def test_autoscale_average_is_used_by_default(self) -> None:
        analyzer = FinOpsAnalyzer(
            pricing_rates=[PricingRate(node_type_id="m5d.large", dbu_rate_per_hour=2.0)]
        )
        run = Run(
            run_id=12,
            job_id=3,
            duration_ms=3_600_000,
            cluster_spec=ClusterSpec(
                node_type_id="m5d.large",
                autoscale=Autoscale(min_workers=2, max_workers=6),
            ),
        )

        result = analyzer.estimate_run_cost(run)

        self.assertIsNotNone(result)
        self.assertAlmostEqual(result.billable_nodes, 5.0)
        self.assertAlmostEqual(result.estimated_cost, 10.0)


if __name__ == "__main__":
    unittest.main()
