import jax

def main() -> None:
    print("Hello from jax-simple!\n")

    # Get devices and print them out.
    devices = jax.devices()

    print("Available devices:")
    for device in devices:
        print(f"  - {device.id}: {device.platform} ({device.device_kind})")
        print(f"    - memory: {device.memory_stats}")
