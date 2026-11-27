# Contributing to cluv

Thank you for your interest in contributing to cluv!

## Issues

Use [GitHub Issues](https://github.com/mila-iqia/cluv/issues) to report bugs or suggest features.

## Before you write code

The maintainers already plan to work on many of the open issues. Please check with us before you
start:

1. **Start from an issue.** To propose a new feature, or to fix a bug that isn't reported yet,
   open an issue first. Small fixes, such as typos, can go straight to a pull request.
2. **Wait until a maintainer assigns the issue to you.** Comment on the issue to say that you'd
   like to work on it. The issue may already be planned, or need a discussion first.
3. **Keep one pull request open at a time.** Wait until your pull request is merged or closed
   before you start on another issue.

Pull requests that skip these steps may be closed without a review.

## Development setup

Clone and install project:
```bash
git clone https://github.com/mila-iqia/cluv
cd cluv
uv sync
```

Use your local version as the cluv tool:
```bash
uv tool install --editable <path_to_cluv_repo>
```

### Testing

```bash
uv run pytest
```

Tests marked `integration` require live SSH connections to real clusters. Skip them locally:

```bash
uv run pytest -m "not integration"
```

### Linting

```bash
uv run pre-commit run --all-files
```

### Docs

Host a server to see changes in real time :
```bash
uv run mkdocs serve --livereload
```
