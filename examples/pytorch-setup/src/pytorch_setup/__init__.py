# Example from : https://github.com/mila-iqia/mila-docs/tree/master/docs/examples/frameworks/pytorch_setup

import torch
import torch.backends

def main():
    cuda_built = torch.backends.cuda.is_built()
    cuda_avail = torch.cuda.is_available()
    device_count = torch.cuda.device_count()

    print(f"PyTorch built with CUDA:         {cuda_built}")
    print(f"PyTorch detects CUDA available:  {cuda_avail}")
    print(f"PyTorch-detected #GPUs:          {device_count}")
    if device_count == 0:
        print("    No GPU detected, not printing devices' names.")
    else:
        for i in range(device_count):
            print(f"    GPU {i}:      {torch.cuda.get_device_name(i)}")
