# Amazon Timestream Tools and Samples Developer Guide

## Pre-Commit Hook

A pre-commit hook configuration is provided in the `.pre-commit-config.yaml` file.

This pre-commit hook is configured to scan for secrets with [`git-secrets`](https://github.com/awslabs/git-secrets), to make sure no secrets are committed to this repository.

To use the pre-commit hook:

1. Install [`pre-commit`](https://pre-commit.com/#install).
2. In the root of this repository, run;
    ```
    pre-commit install
    ```
3. Now, whenever `git commit` is run, `git-secrets` will scan for secrets.

