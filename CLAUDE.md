## Running Commands

Always use `uv` as the task runner with the `--env-file` flag to load environment variables:
```bash
uv run --env-file=.env.local <command>
```

Examples:
- `uv run --env-file=.env.local pytest tests/`
- `uv run --env-file=.env.local python -m bcn.backup ...`

**Note:** Do NOT use `source .env.local && uv run` as `uv run` creates a new shell without inherited environment variables.

## Documentation and Specs

When writing .MD files you must identify if it is more of a study theme or an actual spec. Studies go on docs/study/ specs go on docs/specs/ and must start with a number like "1 - MVP"

There are some study documents on the study folder. They dont represent actual decision backlogs, but they can provide some hints on how to implement the code.

## Code Organization

The code should be created under src/ folder and it should be python.

Before creating a file, assess if it should belong in any of the existing folders rather than the root folder.

## Multi-Stack Support

We must support both the local stack we have in docker-compose.yml and AWS Stack using Glue instead of hive catalog and AWS S3 instead of minio.