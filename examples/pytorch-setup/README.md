# Example : Pytorch Setup

```bash
# At the root of the example folder.
# Run the pytorch-setup command in the job.sh script on the mila cluster.
cluv submit mila scripts/job.sh --gres=gpu:1 --cpus-per-task=2 --mem=16G --time=00:15:00 -- pytorch-setup
```
