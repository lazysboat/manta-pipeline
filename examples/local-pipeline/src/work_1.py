import ray
import mantapipeline.api as manta

@ray.remote
@manta.tag
def work():
    manta.progress_update("...One")
    return "test"

@manta.entrypoint
def work_entrypoint():
    manta.progress_update("Work...")
    result = ray.get(work.remote())
    print(result)

    greeting = manta.get_state("dev-pipeline-x.stage-a.work-0", "greeting")
    manta.progress_update(f"got: {greeting}")

if __name__ == "__main__":
    ray.init()
    work_entrypoint()
    ray.shutdown()
