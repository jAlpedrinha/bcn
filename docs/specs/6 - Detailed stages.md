# Careful, Staged Refactoring Plan: Unified Backup/Restore Architecture

## Core Architectural Insight

There is **ONE backup algorithm** and **ONE restore algorithm**. The only difference is whether previous state exists:

### Backup Algorithm (Universal)
```
1. Check if previous PIT exists
2. If NO: this is first backup → store all files as delta
3. If YES: this is subsequent backup → store only changed files as delta
4. Either way: create PIT with parent reference (null for first, pit_id for rest)
5. Update repository index
```

**Same algorithm. Same code path. Different input state.**

### Restore Algorithm (Universal)
```
1. Determine target PIT (latest if not specified, or specific if provided)
2. Trace PIT chain backwards to root
3. Accumulate files at each step (accounting for adds, mods, deletes)
4. Reconstruct table at target location
```

**Same algorithm regardless of which PIT in the chain.**

---

## Current State (902bd1c)

- `src/bcn/backup.py` - standalone backup script
- `src/bcn/restore.py` - standalone restore script
- No `incremental_backup.py` or `incremental_restore.py`
- No unified CLI

---

## Staged Refactoring: Foundation + CLI (Stages 1-4)

We will unify the core algorithms AND create a single CLI interface. Advanced features (integrity checking, repair strategies, garbage collection) are deferred to future phases for analysis.

### Stage 1: Enhance `backup.py` with Unified Logic

**File: `src/bcn/backup.py`**

**Current State:**
- `IcebergBackup` class creates full backups
- Returns boolean (success/failure)
- No PIT structure, no parent references

**Target State:**
- `IcebergBackup` class works for first AND subsequent backups
- Automatically detects if previous backup exists
- Returns `pit_id` (string) on success, None on failure
- Creates PITs with parent references
- Uses immutable manifests with checksums

**Implementation Pattern:**
```python
class IcebergBackup:
    def create_backup(self) -> Optional[str]:
        """
        Create a backup PIT (Point-in-Time).

        Works for first backup (creates initial PIT with all files as delta)
        and subsequent backups (creates new PIT with only changed files + parent ref).
        Same algorithm - just different input state.

        Returns:
            PIT ID (string) if successful, None otherwise
        """
        # Step 1: Get current table state
        current_state = self._get_table_state()

        # Step 2: Check if previous backup exists
        repository = self._get_or_create_repository()
        previous_pit_id = repository.get_last_pit()

        # Step 3: Determine delta
        if previous_pit_id is None:
            # First backup - ALL files are "added"
            delta = current_state.files
            parent_pit_id = None
        else:
            # Subsequent backup - compare with previous
            previous_state = self._load_state_from_pit(previous_pit_id)
            delta = self._compute_delta(previous_state, current_state)
            parent_pit_id = previous_pit_id

        # Step 4: Create PIT
        pit_id = self._generate_pit_id()
        manifest = self._create_manifest(pit_id, parent_pit_id, delta)

        # Step 5: Store and index
        repository.save_pit(pit_id, manifest)
        repository.update_index(pit_id)

        return pit_id
```

**Tests:**
- `pytest tests/test_backup_restore.py -v`
- Add test: first backup creates PIT with no parent
- Add test: second backup creates PIT with parent reference
- Verify both PITs stored correctly

**Deliverable:**
Single `IcebergBackup` class with unified algorithm

**Status:** All tests PASS ✓

---

### Stage 2: Enhance `restore.py` with Unified Logic

**File: `src/bcn/restore.py`**

**Current State:**
- `IcebergRestore` class restores backups
- No support for specific PIT selection
- No chain traversal

**Target State:**
- `IcebergRestore.restore_backup(pit_id: Optional[str])` method
- If `pit_id` not specified: restore latest PIT
- If `pit_id` specified: restore that specific PIT
- Traces parent chain and accumulates files

**Implementation Pattern:**
```python
class IcebergRestore:
    def restore_backup(self, pit_id: Optional[str] = None) -> bool:
        """
        Restore a table from a PIT backup.

        Works the same way regardless of which PIT is restored.
        Traces parent chain and accumulates files needed.

        Args:
            pit_id: Specific PIT to restore (None = latest)

        Returns:
            True if successful
        """
        repository = self._get_repository()

        # Step 1: Determine target PIT
        if pit_id is None:
            pit_id = repository.get_last_pit()
            if pit_id is None:
                raise ValueError("No PITs found in backup")

        # Step 2: Trace chain and accumulate files
        # This algorithm is the SAME regardless of which PIT we're restoring
        pit_chain = repository.get_pit_chain(pit_id)  # [root -> ... -> pit_id]
        accumulated_files = self._accumulate_files_from_chain(pit_chain)

        # Step 3: Copy files to target location
        self._copy_files(accumulated_files)

        # Step 4: Create metadata and register table
        self._register_table(accumulated_files)

        return True
```

**Tests:**
- `pytest tests/test_backup_restore.py -v`
- Test: restore first (and only) PIT
- Test: create two PITs, restore second PIT (verifies chain traversal)
- Test: restore first PIT from a sequence (shows algorithm works regardless of position)

**Deliverable:**
Single `IcebergRestore` class with unified algorithm accepting optional `pit_id`

**Status:** All tests PASS ✓

---

### Stage 3: Create Unified Repository Helper

**File: `src/bcn/backup.py` - NEW CLASS**

**Current State:**
- PIT management scattered across implementations
- No centralized repository concept

**Target State:**
- `BackupRepository` class in `backup.py`
- Handles: PIT indexing, manifest storage, file accumulation, chain traversal
- Used by both `IcebergBackup` and `IcebergRestore`

**Implementation Pattern:**
```python
class BackupRepository:
    """Manages PIT chain structure and repository state."""

    def __init__(self, backup_name: str, s3_client, backup_bucket: str):
        self.backup_name = backup_name
        self.s3_client = s3_client
        self.backup_bucket = backup_bucket

    def get_or_create_index(self) -> Dict:
        """Get or create repository index (list of PITs)."""
        ...

    def get_last_pit(self) -> Optional[str]:
        """Get latest PIT ID in chain."""
        ...

    def get_pit_chain(self, pit_id: str) -> List[str]:
        """Get ordered chain from root to pit_id."""
        ...

    def save_pit(self, pit_id: str, manifest: Dict) -> None:
        """Store PIT manifest."""
        ...

    def get_pit_manifest(self, pit_id: str) -> Optional[Dict]:
        """Load PIT manifest."""
        ...

    def get_accumulated_files(self, pit_id: str) -> Dict:
        """Accumulate all files from root PIT to target."""
        chain = self.get_pit_chain(pit_id)
        accumulated = {}

        for current_pit in chain:
            manifest = self.get_pit_manifest(current_pit)
            # Apply adds/mods/deletes in order
            accumulated.update(manifest["added_files"])
            accumulated.update(manifest["modified_files"])
            for deleted in manifest["deleted_files"]:
                accumulated.pop(deleted, None)

        return accumulated

    def update_index(self, pit_id: str) -> None:
        """Add PIT to index."""
        ...
```

**Key Concepts:**
- Immutable manifests: once created, never modified
- Parent references: each PIT (except root) has parent_pit_id
- Delta storage: each PIT stores only changed files
- Chain traversal: restore works by tracing backwards

**Tests:**
- `pytest tests/test_backup_restore.py -v`
- Test: creating and storing PITs
- Test: retrieving PIT chain
- Test: file accumulation across chain

**Deliverable:**
`BackupRepository` class managing PIT chains

**Status:** All tests PASS ✓

---

### Stage 4: Create Unified CLI

**File: `src/bcn/cli.py` (NEW)**

**Current State:**
- Standalone `python backup.py` script
- Standalone `python restore.py` script
- No unified interface

**Target State:**
- Single `bcn` command with subcommands
- `bcn backup --database ... --table ... --backup-name ...`
- `bcn restore --backup-name ... --target-database ... --target-table ...`
- `bcn list --backup-name ...`
- `bcn describe --backup-name ...`

**Implementation Pattern:**
```python
class BCNCli:
    """Unified CLI for backup/restore operations."""

    def __init__(self):
        self.parser = self._create_parser()

    def _create_parser(self):
        parser = argparse.ArgumentParser(
            prog='bcn',
            description='BCN - Backup and Restore for Iceberg'
        )

        subparsers = parser.add_subparsers(dest='command')

        # Backup subcommand
        backup_parser = subparsers.add_parser('backup', help='Create backup PIT')
        backup_parser.add_argument('--database', required=True)
        backup_parser.add_argument('--table', required=True)
        backup_parser.add_argument('--backup-name', required=True)
        backup_parser.add_argument('--catalog', default=None)

        # Restore subcommand
        restore_parser = subparsers.add_parser('restore', help='Restore from backup')
        restore_parser.add_argument('--backup-name', required=True)
        restore_parser.add_argument('--target-database', required=True)
        restore_parser.add_argument('--target-table', required=True)
        restore_parser.add_argument('--target-location', required=True)
        restore_parser.add_argument('--pit-id', default=None, help='Specific PIT to restore')

        # List subcommand
        list_parser = subparsers.add_parser('list', help='List PITs in backup')
        list_parser.add_argument('--backup-name', required=True)

        # Describe subcommand
        describe_parser = subparsers.add_parser('describe', help='Describe backup/PIT')
        describe_parser.add_argument('--backup-name', required=True)
        describe_parser.add_argument('--pit-id', default=None)
        describe_parser.add_argument('--verbose', action='store_true')

        return parser

    def run(self, argv=None):
        args = self.parser.parse_args(argv)

        if args.command == 'backup':
            return self._backup(args)
        elif args.command == 'restore':
            return self._restore(args)
        elif args.command == 'list':
            return self._list(args)
        elif args.command == 'describe':
            return self._describe(args)

    def _backup(self, args):
        """Execute backup command."""
        backup = IcebergBackup(
            database=args.database,
            table=args.table,
            backup_name=args.backup_name,
            catalog=args.catalog,
        )
        pit_id = backup.create_backup()
        if pit_id:
            print(f"✓ Backup created: PIT {pit_id}")
            return 0
        else:
            print("✗ Backup failed")
            return 1

    def _restore(self, args):
        """Execute restore command."""
        restore = IcebergRestore(
            backup_name=args.backup_name,
            target_database=args.target_database,
            target_table=args.target_table,
            target_location=args.target_location,
        )
        success = restore.restore_backup(pit_id=args.pit_id)
        if success:
            print(f"✓ Restore completed")
            return 0
        else:
            print("✗ Restore failed")
            return 1

    def _list(self, args):
        """List PITs in backup."""
        repository = BackupRepository(args.backup_name, ...)
        index = repository.get_or_create_index()
        pits = index.get('pits', [])
        print(f"PITs in backup '{args.backup_name}':")
        for pit_id in pits:
            print(f"  - {pit_id}")
        return 0

    def _describe(self, args):
        """Describe backup or PIT."""
        repository = BackupRepository(args.backup_name, ...)
        index = repository.get_or_create_index()
        pits = index.get('pits', [])
        print(f"Backup: {args.backup_name}")
        print(f"Total PITs: {len(pits)}")
        return 0
```

**Entry Point:**
Update `pyproject.toml` to create CLI command:
```toml
[project.scripts]
bcn = "bcn.cli:main"
```

**Tests:**
- `pytest tests/test_backup_restore.py -v`
- Test: `bcn backup ...` creates backup
- Test: `bcn restore ...` restores backup
- Test: `bcn list ...` shows PITs
- Test: `bcn describe ...` shows backup info

**Deliverable:**
Unified `bcn` CLI with subcommands

**Status:** All tests PASS ✓

---

## Summary: What We're Building

After Stages 1-4:
- ✅ Unified backup algorithm (first and subsequent use same code)
- ✅ Unified restore algorithm (any PIT in chain uses same code)
- ✅ PIT chain management (parent references, traversal)
- ✅ Immutable manifests with checksums
- ✅ Delta storage (only changed files per PIT)
- ✅ **Unified CLI with subcommands** (single `bcn` command)

**No more duplication between full/incremental implementations. Single CLI interface.**

---

## Deferred: Advanced Features (Future Phases)

Currently in PR #3 but NOT in this refactoring:

### Feature 1: Integrity Checking
**Purpose:** Detect corruption in backups
- Verify checksums at file, manifest, and PIT levels
- Identify which PIT introduced corruption (binary search)
- Report missing/corrupted files

**Questions for future analysis:**
- How often is corruption actually detected in practice?
- Is binary search the right approach for large backup chains?
- What's the performance impact of full integrity checks?

---

### Feature 2: Repair Strategies
**Purpose:** Automatically recover from corruption
- Skip corrupted PIT and use previous
- Restore from parent PIT
- Manual intervention required

**Questions for future analysis:**
- How should we handle cases where entire chain is corrupted?
- Should we make backups immutable to prevent accidental changes?
- What's the recovery time/cost vs. making fresh backup?

---

### Feature 3: Garbage Collection + Retention Policies
**Purpose:** Clean up old PITs based on retention rules
- Keep last N PITs
- Keep PITs from last X days
- Intelligent deletion (delete oldest first, respecting retention)

**Questions for future analysis:**
- What's a reasonable default retention policy?
- How do we handle "accidental" deletion of recent PITs?
- Should we have immutable "snapshot" PITs that can't be deleted?

---

## Next Steps

1. Implement Stage 1 (unified backup logic)
2. Implement Stage 2 (unified restore logic)
3. Implement Stage 3 (repository helper)
4. Implement Stage 4 (unified CLI)
5. All tests pass (full test suite)
6. Future phase: Plan integrity/repair/GC with concrete examples

---

## Implementation Notes

- **Backwards Compatibility:** Keep old classes temporarily during transition
- **Testing:** Each stage independently testable
- **Commits:** One commit per stage with clear message
- **Examples:** Document with concrete before/after examples
- **Comments:** Add comments explaining unified algorithm
