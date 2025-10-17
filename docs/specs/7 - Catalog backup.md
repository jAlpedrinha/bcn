# Catalog backups

We started the MVP focusing on a single table. We still want to be able to backup and restore a single table, but we should also have the option to backup and restore an entire catalog or database inside a catalog. 

This requires our backup structure and scripts to be aware of catalog name, database name(s) and table name(s). This will also imply a rework on the backup structure that will now include a list of databases and tables which needs to be organized also at the level of the PIT. Question: I have not yet figured out the best structure, can you help with that?

## How to achieve it
 - Equip current CLI to accept catalog name as a parameter.
 - It could still receive database name and table name if needed.
 - Repository integrity must be maintained. 
    - If we backup into an existing repository it can't have be less inclusive than the previous PIT.
    - Tables and databases could have been deleted and that's expected.
    - Problem is when a full catalog backup exists in the repository and we try to make a single table PIT.
    - The above would cause a lot of questions and doubts when doing the next PIT with broader scope.
    - We must keep the solution simple, once a backup repository is created it's granularity is fixed.
    - This also means that if we are performing an incremental backup based on an existing repository we don't necessarily need to explicitly say which catalog/database/table we want.
  - Restore command is more flexible, it can restore with whatever granularity the user wants.
  - Edge cases must be handled:
    - How to handle databases/tables that were deleted
    - Can we identify tables that were renamed? Or will we need to make a full backup for those?
    - What if a specific object suffered no change. How do we reflect that so it doesn't look like it was deleted?
    - Any other edge case ?

## Tooling

 - Describe command should also include repository's granularity
 - Should be able to list all the databases and tables available for a specific repository and PIT.
 

## Testing

There are a lot of tests that can be made based on this spec. It adds a lot of complexity and variables to the solution.

I'm counting on you to help me outline the most complete test suite you can and I'll try to iterate on it. At this point I haven't thought so much about possible implications that I can write a full test suite, but we will evolve around it as we can in the future.