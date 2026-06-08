import ray
import mantapipeline.api as manta

@ray.remote
@manta.tag
def work():
    manta.progress_update("World")
    return "test"

@manta.entrypoint
def work_entrypoint():
    manta.progress_update("Hello")
    manta.progress_update("Test")
    result = ray.get(work.remote())
    print(result)

    params = manta.params()
    dev_int = params["dev-int"]
    manta.progress_update(f"dev-int: {dev_int}")

    manta.set_state("greeting", "hello from work-0")
    manta.set_state("count", 3)

if __name__ == "__main__":
    ray.init()
    work_entrypoint()
    ray.shutdown()
