import time
import ray
import mantapipeline.api as manta

@ray.remote
@manta.tag
def work():
    manta.progress_update("...Long one")

    manta.progress_update("Sleeping for a while")

    time.sleep(60*3)

    manta.progress_update("Done sleeping")

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
