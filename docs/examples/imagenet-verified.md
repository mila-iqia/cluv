# ImageNet example: verified clusters

One badge per cluster the example is configured for, in
[`examples/imagenet/pyproject.toml`][example].

[![mila](https://img.shields.io/github/actions/workflow/status/mila-iqia/cluv/e2e-mila.yaml?branch=master&label=mila)](https://github.com/mila-iqia/cluv/actions/workflows/e2e-mila.yaml)
[![tamia](https://img.shields.io/github/actions/workflow/status/mila-iqia/cluv/e2e-tamia.yaml?branch=master&label=tamia)](https://github.com/mila-iqia/cluv/actions/workflows/e2e-tamia.yaml)
[![killarney](https://img.shields.io/github/actions/workflow/status/mila-iqia/cluv/e2e-killarney.yaml?branch=master&label=killarney)](https://github.com/mila-iqia/cluv/actions/workflows/e2e-killarney.yaml)
[![vulcan](https://img.shields.io/github/actions/workflow/status/mila-iqia/cluv/e2e-vulcan.yaml?branch=master&label=vulcan)](https://github.com/mila-iqia/cluv/actions/workflows/e2e-vulcan.yaml)
[![rorqual](https://img.shields.io/github/actions/workflow/status/mila-iqia/cluv/e2e-rorqual.yaml?branch=master&label=rorqual)](https://github.com/mila-iqia/cluv/actions/workflows/e2e-rorqual.yaml)
[![fir](https://img.shields.io/github/actions/workflow/status/mila-iqia/cluv/e2e-fir.yaml?branch=master&label=fir)](https://github.com/mila-iqia/cluv/actions/workflows/e2e-fir.yaml)
[![nibi](https://img.shields.io/github/actions/workflow/status/mila-iqia/cluv/e2e-nibi.yaml?branch=master&label=nibi)](https://github.com/mila-iqia/cluv/actions/workflows/e2e-nibi.yaml)
[![trillium-gpu](https://img.shields.io/github/actions/workflow/status/mila-iqia/cluv/e2e-trillium-gpu.yaml?branch=master&label=trillium-gpu)](https://github.com/mila-iqia/cluv/actions/workflows/e2e-trillium-gpu.yaml)
[![narval](https://img.shields.io/github/actions/workflow/status/mila-iqia/cluv/e2e-narval.yaml?branch=master&label=narval)](https://github.com/mila-iqia/cluv/actions/workflows/e2e-narval.yaml)

Each badge is the outcome of the last real run of the ImageNet example on that cluster: a GPU job
submitted with `cluv submit` with no job script named on the command line, so the per-cluster
`job_script_path` from the config is what picked the script. The runs happen weekly (Mondays,
06:00 UTC) and on demand, one workflow per cluster.

Click a badge for that cluster's run history - the job id, runtime and full log of every run live
there.

A badge is green only when a job really did train to completion. It goes red if the job failed,
and also if the run could not reach the cluster or the job never finished, so a green badge cannot
quietly mean "we didn't check".

[example]: https://github.com/mila-iqia/cluv/tree/master/examples/imagenet/pyproject.toml
