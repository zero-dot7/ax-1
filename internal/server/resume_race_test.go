package server_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	v1alpha1 "github.com/google/ax/pkg/apis/v1alpha1"
)

// gatedReconciler blocks the first Reconcile call until released, letting the
// test hold a server-side operation in flight while a second one races it.
type gatedReconciler struct {
	mu             sync.Mutex
	reconcileCount int
	started        chan struct{} // closed when the first Reconcile enters
	release        chan struct{} // test closes it to let the first Reconcile finish
}

func (f *gatedReconciler) Reconcile(ctx context.Context, task *v1alpha1.Task, workspaces ...*v1alpha1.Workspace) (*v1alpha1.Task, error) {
	f.mu.Lock()
	f.reconcileCount++
	first := f.reconcileCount == 1
	f.mu.Unlock()

	if first {
		close(f.started)
		select {
		case <-f.release:
		case <-ctx.Done():
			return task, ctx.Err()
		}
	}
	task.Status = &v1alpha1.TaskStatus{
		Phase: task.GetStatus().GetPhase(),
	}
	return task, nil
}

func (f *gatedReconciler) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reconcileCount
}

func (f *gatedReconciler) ReconcileDelete(ctx context.Context, atespace, taskName string) error {
	return nil
}

// TestServer_ResumeNotLostWhenCreateStillReconciling is a regression test for
// the race reported in issue #429: "ax resume right after ax apply can be
// lost". CreateTask reconciles asynchronously with respect to the client; if
// ResumeTask is issued while the create-side reconcile is still in flight, the
// resume must not be silently overwritten by the create's final status write.
// The per-task lock must serialize the two operations so the final observed
// phase is Running, regardless of interleaving.
func TestServer_ResumeNotLostWhenCreateStillReconciling(t *testing.T) {
	rec := &gatedReconciler{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	srv := server.NewServer(memory.NewStore(), server.Options{Reconciler: rec})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const taskName = "task-resume-race"

	// CreateTask blocks inside its first Reconcile, holding the task lock.
	createDone := make(chan error, 1)
	go func() {
		_, err := srv.CreateTask(ctx, &v1alpha1.CreateTaskRequest{
			Task: &v1alpha1.Task{
				Metadata: &v1alpha1.ObjectMeta{Name: taskName},
				Spec:     &v1alpha1.TaskSpec{Image: "alpine"},
			},
		})
		createDone <- err
	}()

	// Wait until the create-side reconcile has actually started.
	select {
	case <-rec.started:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for create-side reconcile to start")
	}

	// Fire ResumeTask while the create is still holding the lock. It must
	// block (not error, not race ahead of the create's status write).
	resumeDone := make(chan error, 1)
	go func() {
		_, err := srv.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Name: taskName})
		resumeDone <- err
	}()

	// Give the resume a moment to pile up on the lock, then let the create finish.
	time.Sleep(50 * time.Millisecond)
	close(rec.release)

	select {
	case err := <-createDone:
		if err != nil {
			t.Fatalf("CreateTask: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for CreateTask")
	}
	select {
	case err := <-resumeDone:
		if err != nil {
			t.Fatalf("ResumeTask: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for ResumeTask — it appears to have been dropped")
	}

	if got := rec.count(); got != 2 {
		t.Errorf("expected 2 reconcile calls (create + resume), got %d", got)
	}

	// The resume must survive the create's final status write: the store's
	// final phase has to be Running, not Suspended.
	got, err := srv.GetTask(ctx, &v1alpha1.GetTaskRequest{Name: taskName})
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.GetStatus().GetPhase() != "Running" {
		t.Errorf("resume lost: final phase is %q, want Running (issue #429)", got.GetStatus().GetPhase())
	}
}
