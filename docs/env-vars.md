# Environment variables

List of environment variables used by the tool.

### [`SKIP_CLEAN_GIT_CHECK`](#skip_clean_git_check)
If set to `1`, skip the check in [`cluv submit`](commands.md/#cluv-submit) that the working git repository
is clean. **You should only use this variable to test changes in your cluv config in `pyproject.toml`, never when. Use the `--autocommit` option instead**.
