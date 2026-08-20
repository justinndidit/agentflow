package state

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCanTransitionTo(t *testing.T) {
	// Every ordered pair of statuses is listed, so adding a status without
	// deciding its transitions makes this table fail to compile-check by
	// omission rather than pass silently.
	tests := []struct {
		from TaskStatus
		to   TaskStatus
		want bool
	}{
		// pending may start or be cancelled before it starts
		{PendingTaskStatus, RunningTaskStatus, true},
		{PendingTaskStatus, CancelledTaskStatus, true},
		{PendingTaskStatus, CompletedTaskStatus, false},
		{PendingTaskStatus, FailedTaskStatus, false},
		{PendingTaskStatus, PendingTaskStatus, false},

		// running may finish any of three ways
		{RunningTaskStatus, CompletedTaskStatus, true},
		{RunningTaskStatus, FailedTaskStatus, true},
		{RunningTaskStatus, CancelledTaskStatus, true},
		{RunningTaskStatus, PendingTaskStatus, false},
		{RunningTaskStatus, RunningTaskStatus, false},

		// failed is the only terminal-looking state that reopens, and it goes
		// straight back to running rather than through pending
		{FailedTaskStatus, RunningTaskStatus, true},
		{FailedTaskStatus, PendingTaskStatus, false},
		{FailedTaskStatus, CompletedTaskStatus, false},
		{FailedTaskStatus, CancelledTaskStatus, false},
		{FailedTaskStatus, FailedTaskStatus, false},

		// completed is terminal
		{CompletedTaskStatus, PendingTaskStatus, false},
		{CompletedTaskStatus, RunningTaskStatus, false},
		{CompletedTaskStatus, FailedTaskStatus, false},
		{CompletedTaskStatus, CancelledTaskStatus, false},
		{CompletedTaskStatus, CompletedTaskStatus, false},

		// cancelled is terminal
		{CancelledTaskStatus, PendingTaskStatus, false},
		{CancelledTaskStatus, RunningTaskStatus, false},
		{CancelledTaskStatus, CompletedTaskStatus, false},
		{CancelledTaskStatus, FailedTaskStatus, false},
		{CancelledTaskStatus, CancelledTaskStatus, false},
	}

	for _, test := range tests {
		t.Run(string(test.from)+"_to_"+string(test.to), func(t *testing.T) {
			task := &Task{Status: test.from}
			if got := task.CanTransitionTo(test.to); got != test.want {
				t.Errorf("CanTransitionTo(%s) from %s = %v, want %v",
					test.to, test.from, got, test.want)
			}
		})
	}
}

// An unrecognised status has no entry in the transition table and must be
// treated as frozen rather than as permitting everything. A task read back with
// a status this build does not know about is the realistic way this happens.
func TestCanTransitionTo_UnknownStatus(t *testing.T) {
	task := &Task{Status: TaskStatus("bogus")}

	for _, to := range []TaskStatus{
		PendingTaskStatus, RunningTaskStatus, CompletedTaskStatus,
		FailedTaskStatus, CancelledTaskStatus,
	} {
		if task.CanTransitionTo(to) {
			t.Errorf("CanTransitionTo(%s) from an unknown status = true, want false", to)
		}
	}
}

func TestIsReady(t *testing.T) {
	tests := []struct {
		name          string
		status        TaskStatus
		remainingDeps int
		want          bool
	}{
		{"pending with no outstanding deps", PendingTaskStatus, 0, true},
		{"pending with one outstanding dep", PendingTaskStatus, 1, false},
		{"running is already claimed", RunningTaskStatus, 0, false},
		{"completed is not re-dispatchable", CompletedTaskStatus, 0, false},
		{"cancelled is not re-dispatchable", CancelledTaskStatus, 0, false},
		// A failed task awaiting retry is moved back to pending before it is
		// ready again; failed on its own is not claimable.
		{"failed is not ready without a reset", FailedTaskStatus, 0, false},
		// Defensive: a counter that has gone negative is a bug upstream, but it
		// must not read as ready.
		{"negative counter is not ready", PendingTaskStatus, -1, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task := &Task{Status: test.status, RemainingDeps: test.remainingDeps}
			if got := task.IsReady(); got != test.want {
				t.Errorf("IsReady() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestCanRetry(t *testing.T) {
	tests := []struct {
		name       string
		attempt    int
		maxRetries int
		want       bool
	}{
		{"no attempts yet with retries allowed", 0, 3, true},
		{"attempts remaining", 2, 3, true},
		{"budget exhausted", 3, 3, false},
		{"past the budget", 4, 3, false},
		{"retries disabled", 0, 0, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task := &Task{Attempt: test.attempt, MaxRetries: test.maxRetries}
			if got := task.CanRetry(); got != test.want {
				t.Errorf("CanRetry() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestIsLeaseExpired(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		expiry *time.Time
		want   bool
	}{
		// An unleased task must not read as expired: the reaper keys off this,
		// and reclaiming a task nobody holds would double-dispatch it.
		{"no lease held", nil, false},
		{"lease expired a minute ago", ptr(now.Add(-time.Minute)), true},
		{"lease valid for another minute", ptr(now.Add(time.Minute)), false},
		// Expiry is exclusive: a lease is live through its expiry instant and
		// expired only after it. The holder gets the full window.
		{"lease expiring exactly now", ptr(now), false},
		{"lease expiring a nanosecond ago", ptr(now.Add(-time.Nanosecond)), true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task := &Task{LeaseExpiry: test.expiry}
			if got := task.IsLeaseExpired(now); got != test.want {
				t.Errorf("IsLeaseExpired() = %v, want %v", got, test.want)
			}
		})
	}
}

// Lease comparison must not be affected by the location a timestamp carries.
// Postgres hands back TIMESTAMPTZ in whatever zone the session is set to.
func TestIsLeaseExpired_ZoneIndependent(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	expiry := now.Add(-time.Minute).In(time.FixedZone("UTC+9", 9*60*60))

	task := &Task{LeaseExpiry: &expiry}
	if !task.IsLeaseExpired(now) {
		t.Error("IsLeaseExpired() = false for an expired lease in another zone, want true")
	}
}

// The status constants are written into a Postgres column typed as the
// task_status enum, so a value added in Go without a matching migration fails
// only at insert time against a real database. Comparing the two here catches
// the drift without one.
func TestTaskStatusConstantsMatchMigration(t *testing.T) {
	goStatuses := []string{
		string(PendingTaskStatus), string(RunningTaskStatus),
		string(CompletedTaskStatus), string(FailedTaskStatus),
		string(CancelledTaskStatus),
	}
	slices.Sort(goStatuses)

	dbStatuses := enumValues(t, "task_status")
	if !slices.Equal(goStatuses, dbStatuses) {
		t.Errorf("task_status enum = %v, Go constants = %v", dbStatuses, goStatuses)
	}
}

func TestWorkflowStatusConstantsMatchMigration(t *testing.T) {
	goStatuses := []string{
		string(PendingWorkflowStatus), string(RunningWorkflowStatus),
		string(CompletedWorkflowStatus), string(FailedWorkflowStatus),
		string(CancelledWorkflowStatus),
	}
	slices.Sort(goStatuses)

	dbStatuses := enumValues(t, "workflow_status")
	if !slices.Equal(goStatuses, dbStatuses) {
		t.Errorf("workflow_status enum = %v, Go constants = %v", dbStatuses, goStatuses)
	}
}

// enumValues pulls the members of a CREATE TYPE ... AS ENUM out of the initial
// migration, sorted.
func enumValues(t *testing.T, typeName string) []string {
	t.Helper()

	path := filepath.Join("..", "..", "migrations", "000001_init_db.up.sql")
	sql, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read %s: %v", path, err)
	}

	pattern := regexp.MustCompile(
		`CREATE TYPE\s+` + regexp.QuoteMeta(typeName) + `\s+AS ENUM\s*\(([^)]*)\)`)
	match := pattern.FindSubmatch(sql)
	if match == nil {
		t.Fatalf("no CREATE TYPE %s found in %s", typeName, path)
	}

	values := []string{}
	for _, raw := range strings.Split(string(match[1]), ",") {
		values = append(values, strings.Trim(strings.TrimSpace(raw), "'"))
	}
	slices.Sort(values)
	return values
}

// A zero Task is what a struct literal in the scheduling path starts from, so
// its defaults should be inert rather than accidentally claimable.
func TestZeroTaskIsNotReady(t *testing.T) {
	var task Task

	if task.IsReady() {
		t.Error("a zero Task reads as ready; its status should not be pending by default")
	}
	if task.IsLeaseExpired(time.Now()) {
		t.Error("a zero Task reads as having an expired lease")
	}
	if task.CanRetry() {
		t.Error("a zero Task reads as retryable")
	}
	if task.ID != uuid.Nil {
		t.Error("a zero Task has a non-nil ID")
	}
}

func ptr[T any](v T) *T { return &v }
