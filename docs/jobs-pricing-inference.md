# Jobs Pricing Inference

Facebrick currently infers Databricks Jobs cost from run duration plus manually configured per-node-type DBU settings.

## What the app uses

For each run, the backend tries to resolve:

- The run duration in milliseconds
- The worker node type
- The driver node type
- The worker count

Those come from Databricks job/run/cluster metadata already synced into the app.

Separately, the Config tab stores for each node type:

- `dbus_per_hour`
- `plan`: `premium` or `enterprise`

## How the Jobs hourly rate is derived

For Jobs workloads, the app converts DBUs to hourly cost using:

- `premium`: `jobs_rate_per_hour = dbus_per_hour * 0.15`
- `enterprise`: `jobs_rate_per_hour = dbus_per_hour * 0.20`

This is done per node type.

The UI also shows all-purpose reference pricing:

- `premium`: `all_purpose_rate_per_hour = dbus_per_hour * 0.55`
- `enterprise`: `all_purpose_rate_per_hour = dbus_per_hour * 0.65`

But the current FinOps dashboard uses the Jobs rate, not the all-purpose rate.

## How run cost is inferred

Yes: the current model is essentially:

`run cost = run duration in hours * cluster hourly price`

More precisely:

`run cost = (worker_count * worker_jobs_rate + driver_count * driver_jobs_rate) * duration_hours`

Where:

- `worker_count` comes from `num_workers`, or from autoscaling metadata using the analyzer strategy
- `driver_count` is `1` when driver pricing is included
- `worker_jobs_rate` comes from the configured worker node type
- `driver_jobs_rate` comes from the configured driver node type
- `duration_hours = duration_ms / 3,600,000`

If the driver node type is not explicitly present, the app falls back to the worker node type for the driver.

## Example

If a run used:

- worker node type `m5d.large`
- driver node type `m5d.large`
- `num_workers = 2`
- configured `dbus_per_hour = 2.0`
- `plan = premium`
- duration `2 hours`

Then:

- Jobs rate per node = `2.0 * 0.15 = 0.30 USD/hour`
- Billable nodes = `2 workers + 1 driver = 3`
- Cluster hourly Jobs price = `3 * 0.30 = 0.90 USD/hour`
- Run cost = `0.90 * 2 = 1.80 USD`

## Important limitations

- This is an estimate based on runtime and configured DBUs, not a direct Databricks billing export.
- Infrastructure cost is not currently added.
- The dashboard only prices runs whose node types exist in the saved Config tab.
- If a run cannot resolve a cluster spec or pricing entry, it stays uncosted.
- Autoscaling runs are estimated from the configured autoscale strategy, not from per-minute worker history.

## Source in code

The main logic is in:

- [finops_service.py](/home/condo-142/dev/facebricks/src/app/finops_service.py)
- [analyzer.py](/home/condo-142/dev/facebricks/src/finops/analyzer.py)
