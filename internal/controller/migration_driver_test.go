/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

const migrationDriverTestWorkload = "web"

type fakeMigrationEngine struct {
	requestedDestination MigrationDestination
	requestedKey         types.NamespacedName
	progress             []MigrationProgress
	steps                int
	requestErr           error
	inspectErr           error
	inspectErrorAt       int
	inspectCalls         int
	stepErr              error
}

func (f *fakeMigrationEngine) RequestMigration(
	_ context.Context,
	key types.NamespacedName,
	destination MigrationDestination,
) error {
	f.requestedKey = key
	f.requestedDestination = destination
	return f.requestErr
}

func (f *fakeMigrationEngine) InspectMigration(
	_ context.Context,
	_ types.NamespacedName,
	_ MigrationDestination,
) (MigrationProgress, error) {
	f.inspectCalls++
	if f.inspectErr != nil && f.inspectCalls == f.inspectErrorAt {
		return MigrationProgress{}, f.inspectErr
	}
	index := min(f.steps, len(f.progress)-1)
	return f.progress[index], nil
}

func (f *fakeMigrationEngine) StepMigration(
	_ context.Context,
	_ types.NamespacedName,
) (MigrationStep, error) {
	f.steps++
	return MigrationStep{}, f.stepErr
}

func TestMigrationDriverRunsSharedEngineToCompletion(t *testing.T) {
	t.Parallel()

	key := types.NamespacedName{Namespace: "team", Name: migrationDriverTestWorkload}
	engine := &fakeMigrationEngine{progress: []MigrationProgress{
		{
			Destination:       MigrationDestinationNative,
			CurrentController: MigrationDestinationChurnless,
			Message:           "waiting to start handoff to native Kubernetes",
		},
		{
			Destination:       MigrationDestinationNative,
			CurrentController: MigrationDestinationChurnless,
			Phase:             migrationPhaseWarming,
			Mode:              migrationModePreserve,
			Message:           "preserve handoff in warming phase",
		},
		{
			Destination:       MigrationDestinationNative,
			CurrentController: MigrationDestinationNative,
			Complete:          true,
			Message:           "handoff complete; native Kubernetes is authoritative",
		},
	}}
	var observed []MigrationProgress
	driver := &MigrationDriver{
		Engine:  engine,
		Observe: func(progress MigrationProgress) { observed = append(observed, progress) },
	}

	if err := driver.Transfer(
		context.Background(),
		key,
		MigrationDestinationNative,
	); err != nil {
		t.Fatal(err)
	}
	if engine.requestedKey != key {
		t.Fatalf("requested key = %v, want %v", engine.requestedKey, key)
	}
	if engine.requestedDestination != MigrationDestinationNative {
		t.Fatalf("requested destination = %q", engine.requestedDestination)
	}
	if engine.steps != 2 {
		t.Fatalf("reconcile steps = %d, want 2", engine.steps)
	}
	if len(observed) != 3 || !observed[len(observed)-1].Complete {
		t.Fatalf("observed progress = %#v", observed)
	}
}

func TestMigrationDriverIsIdempotentAtDestination(t *testing.T) {
	t.Parallel()

	engine := &fakeMigrationEngine{progress: []MigrationProgress{{
		Destination:       MigrationDestinationChurnless,
		CurrentController: MigrationDestinationChurnless,
		Complete:          true,
		Message:           "takeover complete; Churnless is authoritative",
	}}}
	driver := &MigrationDriver{Engine: engine}
	if err := driver.Transfer(
		context.Background(),
		types.NamespacedName{
			Namespace: migrationTestNamespace,
			Name:      migrationDriverTestWorkload,
		},
		MigrationDestinationChurnless,
	); err != nil {
		t.Fatal(err)
	}
	if engine.steps != 0 {
		t.Fatalf("reconcile steps = %d, want 0", engine.steps)
	}
}

func TestMigrationDriverTimeoutExplainsResume(t *testing.T) {
	t.Parallel()

	engine := &fakeMigrationEngine{progress: []MigrationProgress{{
		Destination:       MigrationDestinationNative,
		CurrentController: MigrationDestinationChurnless,
		Message:           "waiting for native ReplicaSet",
	}}}
	driver := &MigrationDriver{
		Engine:       engine,
		PollInterval: time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := driver.Transfer(
		ctx,
		types.NamespacedName{
			Namespace: migrationTestNamespace,
			Name:      migrationDriverTestWorkload,
		},
		MigrationDestinationNative,
	)
	if err == nil || !strings.Contains(err.Error(), "run the same command to resume") {
		t.Fatalf("error = %v", err)
	}
}

func TestMigrationDriverInterruptionAlwaysExplainsResume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		engine *fakeMigrationEngine
	}{
		{
			name: "request",
			engine: &fakeMigrationEngine{
				requestErr: context.DeadlineExceeded,
			},
		},
		{
			name: "inspect",
			engine: &fakeMigrationEngine{
				progress:       []MigrationProgress{{Message: "starting handoff"}},
				inspectErr:     context.DeadlineExceeded,
				inspectErrorAt: 1,
			},
		},
		{
			name: "step",
			engine: &fakeMigrationEngine{
				progress: []MigrationProgress{{
					Destination:       MigrationDestinationNative,
					CurrentController: MigrationDestinationChurnless,
					Message:           "warming native target",
				}},
				stepErr: context.DeadlineExceeded,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			driver := &MigrationDriver{Engine: tt.engine}
			err := driver.Transfer(
				context.Background(),
				types.NamespacedName{
					Namespace: migrationTestNamespace,
					Name:      migrationDriverTestWorkload,
				},
				MigrationDestinationNative,
			)
			if err == nil ||
				!strings.Contains(err.Error(), "cluster state may have advanced") ||
				!strings.Contains(err.Error(), "run the same command to resume") {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestMigrationDriverStopsWhenRequestIsSuperseded(t *testing.T) {
	t.Parallel()

	engine := &fakeMigrationEngine{progress: []MigrationProgress{
		{
			Destination:         MigrationDestinationNative,
			RequestedController: MigrationDestinationNative,
			CurrentController:   MigrationDestinationChurnless,
			Message:             "waiting to start handoff",
		},
		{
			Destination:         MigrationDestinationNative,
			RequestedController: MigrationDestinationChurnless,
			CurrentController:   MigrationDestinationChurnless,
			Superseded:          true,
			Message:             "request to native was superseded by desired controller churnless",
		},
	}}
	driver := &MigrationDriver{Engine: engine}
	err := driver.Transfer(
		context.Background(),
		types.NamespacedName{
			Namespace: migrationTestNamespace,
			Name:      migrationDriverTestWorkload,
		},
		MigrationDestinationNative,
	)
	if err == nil || !strings.Contains(err.Error(), "superseded") {
		t.Fatalf("error = %v", err)
	}
	if engine.steps != 1 {
		t.Fatalf("reconcile steps = %d, want 1", engine.steps)
	}
}
