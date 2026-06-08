import ray
import mantapipeline.api as manta

@ray.remote
@manta.tag
def work():
    manta.progress_update("...Three")

    for i in range(10):
        manta.progress_update(f"Loop {i}")

    return "test"

@manta.entrypoint
def work_entrypoint():
    manta.progress_update("Work...")
    result = ray.get(work.remote())
    print(result)

    count = manta.get_state("dev-pipeline-x.stage-a.work-0", "count")
    manta.progress_update(f"count={count} ({type(count).__name__})")

if __name__ == "__main__":
    ray.init()
    work_entrypoint()
    ray.shutdown()
