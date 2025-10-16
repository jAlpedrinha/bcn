When writing .MD files you must identify it it is more of a study theme or an actual spec. Studies go on study/ specs go on specs/ and must start with a number like "1 - MVP"

There are some study documents on the study folder. They dont represent actual decision backlogs, but they can provide some hints on how to implement the code.

The code should be created under src/ folder and it should be python.

Before creating a file, assess if it should belong in any of the existing folders rather than the root folder.

We must support both the local stack we have in docker-compose.yml and AWS Stack using Glue instead of hive catalog and AWS S3 instead of minio.