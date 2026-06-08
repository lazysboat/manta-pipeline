import time

import ray
import mantapipeline.api as manta

@ray.remote
@manta.tag
def work():
    manta.progress_update("...Two")

    for i in range(10):
        manta.progress_update(f"Loop {i}")

    manta.progress_update("Starting float bar")

    for i in range(1000):
        f = float(i) / 1000
        manta.progress_bar("float bar", f, 0.0, 1.0)
        time.sleep(0.01)

    manta.progress_update("Starting int bar")

    for i in range(64):
        manta.progress_bar("int bar", i, 0, 64)
        time.sleep(0.1)

    manta.progress_update("Bars done")

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
