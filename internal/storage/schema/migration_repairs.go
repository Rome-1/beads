package schema

import (
	"context"
	"fmt"
)

// Pre-migration repairs run immediately before a specific pending migration
// file is applied. Shipped migration files are frozen (see
// scripts/check-migration-hygiene.sh): editing one forks fresh clones from
// upgraded clones via the recorded content hash, and a bug that makes a
// migration FAIL on drifted databases cannot be fixed forward with a new
// migration either — the failing file aborts the pass before any later
// version runs. Repairing the drift in code, keyed to the pending version,
// is the only path that heals affected databases without touching shipped
// SQL. Precedent: ensureContentHashColumn, the aux row-id backfill.

// repairKey identifies a pre-migration repair by its migration source cursor
// table and the pending version it runs before.
type repairKey struct {
	cursorTable string
	version     int
}

// preMigrationRepairs is the registry of repairs that must run immediately
// before a specific migration file is applied. It is keyed rather than a switch
// so TestFrozenNonIdempotentMigrationsHaveRepairs can assert that every shipped
// migration whose frozen body cannot replay on its own (0040/0041, which write
// dolt_nonlocal_tables and self-commit) has a repair registered here.
var preMigrationRepairs = map[repairKey]func(context.Context, DBConn) error{
	{"schema_migrations", 40}: repairPartial0040NonlocalInsert,
	{"schema_migrations", 41}: repairPartial0041NonlocalDelete,
	{"schema_migrations", 53}: repairV53RigAndSplitTargets,
}

// preMigrationRepair dispatches any repair registered for (source, version).
func (m migrationSource) preMigrationRepair(ctx context.Context, db DBConn, version int) error {
	if repair, ok := preMigrationRepairs[repairKey{m.cursorTable, version}]; ok {
		return repair(ctx, db)
	}
	return nil
}

// repairV53RigAndSplitTargets is the pre-0053 repair: it backfills the issues
// rig/agent columns (#4502) and the wisp_dependencies split-target columns
// (#4555) that 0053 reads.
func repairV53RigAndSplitTargets(ctx context.Context, db DBConn) error {
	if err := ensureIssuesRigColumns(ctx, db); err != nil {
		return err
	}
	return ensureWispDependenciesSplitTargets(ctx, db)
}

// The four dolt_nonlocal_tables rows migration 0040 inserts (and migration 0041
// clears). Kept as a single source of truth for the version-40/41 replay
// repairs, which normalise the table to the exact state each shipped
// (content-hashed, un-editable) migration body expects before it runs.
const nonlocalFrozenRowsInList = "('wisps', 'wisp_*', 'repo_mtimes', 'local_metadata')"
const nonlocalFrozenRowsValues = "('wisps', 'main', 'immediate'), ('wisp_*', 'main', 'immediate'), " +
	"('repo_mtimes', 'main', 'immediate'), ('local_metadata', 'main', 'immediate')"

// anyNonlocalFrozenRowPresent reports whether any of 0040's four
// dolt_nonlocal_tables rows currently exists (in the working set), the signal
// the version-40/41 repairs use to decide whether a heal is needed. Guarding on
// it keeps both repairs a strict no-op on the common (non-partial) path — which
// also matters because most migrations do not self-commit, so an unconditional
// `DOLT_COMMIT -A` here would prematurely sweep the pending working set into a
// repair commit on a fresh init.
func anyNonlocalFrozenRowPresent(ctx context.Context, db DBConn) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM dolt_nonlocal_tables WHERE table_name IN "+nonlocalFrozenRowsInList).Scan(&count); err != nil {
		return false, fmt.Errorf("counting nonlocal frozen rows: %w", err)
	}
	return count > 0, nil
}

// repairPartial0040NonlocalInsert heals a partially-applied migration 0040 so
// its shipped body can replay. 0040 bare-INSERTs four dolt_nonlocal_tables rows,
// each paired with its own CALL DOLT_COMMIT. Over a shared sql-server a transient
// ("busy buffer" -> "bad connection") can leave some rows committed while the
// schema_migrations version row never records, so the init retry loop re-runs
// 0040 from the top and the bare INSERT dies on "duplicate primary key given:
// [wisps]", bricking the database. 0040 is a shipped, content-hashed migration
// and cannot be edited (see the file header), so instead clear any of those four
// rows before the replay and commit the removal, leaving 0040's INSERT+COMMIT
// pairs a clean, real diff. No-op when 0040 never partially applied.
func repairPartial0040NonlocalInsert(ctx context.Context, db DBConn) error {
	present, err := anyNonlocalFrozenRowPresent(ctx, db)
	if err != nil {
		return err
	}
	if !present {
		return nil
	}
	if _, err := db.ExecContext(ctx,
		"DELETE FROM dolt_nonlocal_tables WHERE table_name IN "+nonlocalFrozenRowsInList); err != nil {
		return fmt.Errorf("clearing partial 0040 nonlocal rows: %w", err)
	}
	if err := drainCall(ctx, db,
		"CALL DOLT_COMMIT('-Am', 'repair: clear partial 0040 nonlocal rows before replay', '--skip-empty')"); err != nil {
		return fmt.Errorf("committing 0040 nonlocal repair: %w", err)
	}
	return nil
}

// repairPartial0041NonlocalDelete heals a partially-applied migration 0041 so
// its shipped body can replay. 0041 begins by clearing dolt_nonlocal_tables and
// committing ("disable nonlocal tables for fk migrations"). If a transient
// interrupts 0041 after that commit but before its version row records, the
// retry re-runs 0041 against an already-empty table: the DELETE stages nothing
// and the paired DOLT_COMMIT (no --skip-empty in the shipped bytes) fails with
// "nothing to commit". Restore the pre-0041 invariant — the four rows 0040
// leaves — so the frozen DELETE+COMMIT has a real diff again. No-op when those
// rows are already present (the common path).
func repairPartial0041NonlocalDelete(ctx context.Context, db DBConn) error {
	present, err := anyNonlocalFrozenRowPresent(ctx, db)
	if err != nil {
		return err
	}
	if present {
		return nil
	}
	if _, err := db.ExecContext(ctx,
		"INSERT IGNORE INTO dolt_nonlocal_tables (table_name, target_ref, options) VALUES "+nonlocalFrozenRowsValues); err != nil {
		return fmt.Errorf("restoring pre-0041 nonlocal rows: %w", err)
	}
	if err := drainCall(ctx, db,
		"CALL DOLT_COMMIT('-Am', 'repair: restore pre-0041 nonlocal rows before replay', '--skip-empty')"); err != nil {
		return fmt.Errorf("committing 0041 nonlocal repair: %w", err)
	}
	return nil
}

// ensureIssuesRigColumns repairs #4502: the rig/agent columns were only ever
// added to the squashed bootstrap 0001_create_issues, so a database
// bootstrapped before they existed reaches schema v52 without them, and
// migration 0053 — which copies exactly these columns from wisps into
// issues — fails with "Unknown column" even with zero rig wisps to repair.
// Databases in the wild may have some but not all six, so each is checked
// individually. Definitions mirror the current bootstrap schema.
func ensureIssuesRigColumns(ctx context.Context, db DBConn) error {
	columns := []struct{ name, definition string }{
		{"hook_bead", "VARCHAR(255) DEFAULT ''"},
		{"role_bead", "VARCHAR(255) DEFAULT ''"},
		{"agent_state", "VARCHAR(32) DEFAULT ''"},
		{"last_activity", "DATETIME"},
		{"role_type", "VARCHAR(32) DEFAULT ''"},
		{"rig", "VARCHAR(255) DEFAULT ''"},
	}
	for _, col := range columns {
		present, err := schemaColumnExists(ctx, db, "issues", col.name)
		if err != nil {
			return fmt.Errorf("checking issues.%s: %w", col.name, err)
		}
		if present {
			continue
		}
		if _, err := db.ExecContext(ctx, "ALTER TABLE issues ADD COLUMN "+col.name+" "+col.definition); err != nil {
			return fmt.Errorf("adding issues.%s for migration 0053: %w", col.name, err)
		}
	}
	return nil
}

// ensureWispDependenciesSplitTargets repairs #4555: a mixed-vintage local
// wisp_dependencies table can have the post-0005 id column while still lacking
// one or more split target columns. Migration 0053 reads those columns when it
// repairs rig wisps, so add the missing columns and backfill them from the
// legacy depends_on_id column when that source column is still available.
func ensureWispDependenciesSplitTargets(ctx context.Context, db DBConn) error {
	table, err := schemaTableExists(ctx, db, "wisp_dependencies")
	if err != nil {
		return fmt.Errorf("checking wisp_dependencies table: %w", err)
	}
	if !table {
		return nil
	}

	columns := wispDependenciesSplitTargetColumns()
	missing := make([]struct{ name, definition string }, 0, len(columns))
	for _, col := range columns {
		present, err := schemaColumnExists(ctx, db, "wisp_dependencies", col.name)
		if err != nil {
			return fmt.Errorf("checking wisp_dependencies.%s: %w", col.name, err)
		}
		if !present {
			missing = append(missing, col)
		}
	}
	if len(missing) == 0 {
		return nil
	}

	for _, col := range missing {
		if _, err := db.ExecContext(ctx, "ALTER TABLE wisp_dependencies ADD COLUMN "+col.name+" "+col.definition); err != nil {
			return fmt.Errorf("adding wisp_dependencies.%s for migration 0053: %w", col.name, err)
		}
	}

	legacyTarget, err := schemaColumnExists(ctx, db, "wisp_dependencies", "depends_on_id")
	if err != nil {
		return fmt.Errorf("checking wisp_dependencies.depends_on_id: %w", err)
	}
	if !legacyTarget {
		return nil
	}

	for _, repair := range wispDependenciesSplitTargetBackfillSQL() {
		if _, err := db.ExecContext(ctx, repair); err != nil {
			return fmt.Errorf("backfilling wisp_dependencies split targets for migration 0053: %w", err)
		}
	}
	return nil
}

func wispDependenciesSplitTargetColumns() []struct{ name, definition string } {
	return []struct{ name, definition string }{
		{"depends_on_issue_id", "VARCHAR(255) NULL"},
		{"depends_on_wisp_id", "VARCHAR(255) NULL"},
		{"depends_on_external", "VARCHAR(255) NULL"},
	}
}

func wispDependenciesSplitTargetBackfillSQL() []string {
	return []string{
		"UPDATE wisp_dependencies SET depends_on_external = depends_on_id WHERE depends_on_external IS NULL AND depends_on_id LIKE 'external:%'",
		"UPDATE wisp_dependencies wd JOIN wisps w ON w.id = wd.depends_on_id SET wd.depends_on_wisp_id = wd.depends_on_id WHERE wd.depends_on_wisp_id IS NULL AND wd.depends_on_external IS NULL",
		"UPDATE wisp_dependencies wd JOIN issues i ON i.id = wd.depends_on_id SET wd.depends_on_issue_id = wd.depends_on_id WHERE wd.depends_on_issue_id IS NULL AND wd.depends_on_external IS NULL AND wd.depends_on_wisp_id IS NULL",
		"UPDATE wisp_dependencies SET depends_on_external = depends_on_id WHERE depends_on_external IS NULL AND depends_on_wisp_id IS NULL AND depends_on_issue_id IS NULL",
	}
}

func schemaTableExists(ctx context.Context, db DBConn, table string) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
	`, table).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func schemaColumnExists(ctx context.Context, db DBConn, table, column string) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?
	`, table, column).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}
