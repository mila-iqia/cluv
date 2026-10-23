# Troubleshooting

This page covers common setup issues.

## Setting up SSH access to all clusters

`cluv` connects to each cluster using the hostnames configured in your `~/.ssh/config`. We
recommend using `milatools` to generate the SSH config for all Mila and Alliance clusters at once:

```bash
mila init
```

!!! note

    `mila init` is meant to be run on your **local machine**.

More information in the Mila Technical Documentation:

- [Get Started with the Cluster](https://docs.mila.quebec/getting_started/)
- [Digital Research Alliance of Canada Clusters](https://docs.mila.quebec/technical_reference/clusters/)

## Setting up SSH access to GitHub from the clusters

`cluv sync` clones and pulls your repository *from the clusters themselves*, so each cluster needs
its own way to authenticate to GitHub. This matters because most git repositories are private:
without proper authentication, the clone/pull steps on the clusters will simply fail.
There are two ways to set this up.

### Option 1 - Forward your local SSH agent (recommended)

If you already have a GitHub SSH key on your machine, forward it through the SSH connection
instead of creating a new key on every cluster. In your local `~/.ssh/config`:

```
Host mila* narval tamia rorqual fir
    ForwardAgent yes
```

Then, before running `cluv login` / `cluv sync`, make sure your key is loaded in the agent:

```bash
ssh-add <your-github-ssh-key>
```

Alternatively, add `AddKeysToAgent yes` to your SSH config to load keys automatically on first use.

### Option 2 - Create a dedicated key on each cluster

Otherwise, generate and register an SSH key on each cluster you want to use:

1. Connect to the cluster:

    ```bash
    ssh <cluster>
    ```

2. Generate a new SSH key-pair (accept the defaults):

    ```bash
    ssh-keygen -t rsa -b 3072
    ```

3. Add the public key (`~/.ssh/id_rsa.pub`) to your GitHub account:

    - Through the GitHub website: <https://github.com/settings/keys>
    - Or with the GitHub CLI: `gh ssh-key add ~/.ssh/id_rsa.pub`

### Verifying

Either way, you can check that GitHub authentication works from a cluster:

```bash
ssh -T git@github.com
```

You should see a `Hi <username>! You've successfully authenticated` message.
