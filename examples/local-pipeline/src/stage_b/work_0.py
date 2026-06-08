import ray
import mantapipeline.api as manta

@ray.remote
@manta.tag
def work():
    manta.progress_update("...From Stage B")

    return "test"

@manta.entrypoint
def work_entrypoint():
    manta.progress_update("Work...")
    result = ray.get(work.remote())
    print(result)

if __name__ == "__main__":
    ray.init()
    work_entrypoint()
    ray.shutdown()
