# Manta Pipeline

Implements single definition paradigm, enabling easy orchestration and observability of end-to-end ML pipelines. Open source extension for [Ray compute engine](https://www.ray.io/)

**This is minimal dev preview for demonstration puroses** (`0.1.0.dev1`)

Ubuntu/Linux only. Requires [`uv`](https://docs.astral.sh/uv/).

## Install

**CLI** (Ubuntu):

```sh
curl -fsSL https://github.com/lazysboat/manta-pipeline/releases/latest/download/install.sh | sh
manta-pipeline version
```

**Python API** (for authoring pipeline works):

```sh
pip install "mantapipeline @ git+https://github.com/lazysboat/manta-pipeline.git#subdirectory=python-api"
```

## Get started

Run the bundled local example:

```sh
cd examples/local-pipeline
manta-pipeline up                  # start a local Ray head
manta-pipeline build               # build the pipeline
manta-pipeline run local-pipeline  # run it
manta-pipeline down                # stop
```
