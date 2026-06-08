import time
import ray
import mantapipeline.api as manta


@ray.remote(num_gpus=1)
@manta.tag  # 4. Tag remote function/method to work context
def cuda_check():
    import torch # TODO: WHY NEEDED HERE NOT GLOBAL
    manta.progress_update("Starting CUDA check")

    # 1) Basic availability
    available = torch.cuda.is_available()
    manta.progress_update(f"cuda available: {available}")
    manta.progress_update(f"torch version: {torch.__version__}")
    manta.progress_update(f"cuda build: {torch.version.cuda}")
    manta.progress_update(f"cudnn version: {torch.backends.cudnn.version()}")

    if not available:
        manta.progress_update("CUDA NOT available, aborting GPU test")
        return {"cuda": False}

    # 2) Device inventory
    count = torch.cuda.device_count()
    manta.progress_update(f"device count: {count}")
    for i in range(count):
        props = torch.cuda.get_device_properties(i)
        mem_gib = props.total_memory / 1024**3
        manta.progress_update(f"dev {i} name: {props.name}")
        manta.progress_update(f"dev {i} cc: {props.major}.{props.minor}")
        manta.progress_update(f"dev {i} mem: {mem_gib:.1f} GiB")
        manta.progress_update(f"dev {i} sms: {props.multi_processor_count}")

    device = torch.device("cuda:0")

    # 3) Sustained GPU stress test (~45s)
    # Enable TF32 to exercise tensor cores on Ampere+ GPUs
    torch.backends.cuda.matmul.allow_tf32 = True
    torch.backends.cudnn.allow_tf32 = True

    # Size matrices to ~15% of GPU memory each, rounded to a tensor-core-friendly multiple
    total_mem = torch.cuda.get_device_properties(device).total_memory
    max_n = int((total_mem * 0.15 / 4) ** 0.5)  # 4 bytes per fp32
    N = min(max(4096, (max_n // 1024) * 1024), 16384)
    target_seconds = 45.0
    batch = 20  # matmuls queued between syncs

    manta.progress_update(f"stress target: {target_seconds:.0f}s")
    manta.progress_update(f"matrix size: {N}x{N}")
    manta.progress_update(f"batch per sync: {batch}")

    a = torch.randn(N, N, device=device)
    b = torch.randn(N, N, device=device)

    # Warmup — first matmul triggers cuBLAS init and kernel selection
    for _ in range(3):
        a = torch.matmul(a, b)
    a = a / (a.norm() + 1e-8)
    torch.cuda.synchronize()

    # Per-window throughput tracking to catch thermal throttling
    window_start = time.time()
    window_iters = 0
    min_tflops = float("inf")
    max_tflops = 0.0

    t0 = time.time()
    iters = 0
    last_progress = 0.0
    while True:
        # Queue `batch` matmuls, chained so the scheduler can't reorder them away
        for _ in range(batch):
            a = torch.matmul(a, b)
        # Renormalize to keep values bounded; this also acts as the device-side barrier
        a = a / (a.norm() + 1e-8)
        torch.cuda.synchronize()
        iters += batch
        window_iters += batch

        now = time.time()
        elapsed = now - t0
        window_elapsed = now - window_start
        if window_elapsed >= 5.0:
            win_tflops = (2 * N**3 * window_iters) / window_elapsed / 1e12
            min_tflops = min(min_tflops, win_tflops)
            max_tflops = max(max_tflops, win_tflops)
            manta.progress_update(f"window tflops: {win_tflops:.2f}")
            window_start = now
            window_iters = 0

        manta.progress_bar("gpu seconds", int(elapsed), 0, int(target_seconds))

        p = min(elapsed / target_seconds, 1.0)
        if p - last_progress >= 0.01 or p >= 1.0:
            last_progress = p
        if elapsed >= target_seconds:
            break

    elapsed = time.time() - t0
    tflops = (2 * N**3 * iters) / elapsed / 1e12
    throttle_ratio = (min_tflops / max_tflops) if max_tflops > 0 else 0.0
    manta.progress_update(f"min window tflops: {min_tflops:.2f}")
    manta.progress_update(f"max window tflops: {max_tflops:.2f}")
    manta.progress_update(f"throttle ratio: {throttle_ratio:.2f}")

    # 5) Memory usage
    alloc_mib = torch.cuda.memory_allocated(device) / 1024**2
    reserved_mib = torch.cuda.memory_reserved(device) / 1024**2
    manta.progress_update(f"gpu mem allocated: {alloc_mib:.1f} MiB")
    manta.progress_update(f"gpu mem reserved: {reserved_mib:.1f} MiB")

    manta.progress_update("Done CUDA check")
    return {
        "cuda": True,
        "devices": count,
        "elapsed_s": elapsed,
    }


@manta.entrypoint  # 1. Work entrypoint initializes work context
def combine_data():
    manta.progress_update("Starting combine_data")

    if ray.available_resources().get("GPU", 0) < 1:
        manta.progress_update("No GPU available, skipping CUDA check")
        return

    result = ray.get(cuda_check.remote())
    #manta.progress_update(f"CUDA check result: {result}")
    manta.progress_update("Done")


if __name__ == "__main__":
    ray.init()  # 0. Ray up and running
    combine_data()
    ray.shutdown()