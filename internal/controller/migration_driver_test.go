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
	"errors"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

const migrationDriverTestWorkload = "web"

type fakeMigrationEngine struct {
	requestedDestination MigrationDestination
	requestedKey         types.NamespacedName
	progress             []MigrationProgress
	steps                int
	requestErr           error
	advanceErr           error
	advanceErrorAt       int
	advanceCalls         int
	stepResult           MigrationStep
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

func (f *fakeMigrationEngine) AdvanceMigration(
	_ context.Context,
	_ types.NamespacedName,
	_ MigrationDestination,
) (MigrationProgress, MigrationStep, error) {
	f.advanceCalls++
	if f.advanceErr != nil && f.advanceCalls == f.advanceErrorAt {
		return MigrationProgress{}, 0, f.advanceErr
	}
	index := min(f.steps, len(f.progress)-1)
	progress := f.progress[index]
	if progress.Complete || progress.Superseded {
		return progress, 0, nil
	}
	f.steps++
	return progress, f.stepResult, f.stepErr
}

func TestMigrationDriverTransfer(t *testing.T) {
	t.Parallel()

	key := types.NamespacedName{Namespace: "team", Name: migrationDriverTestWorkload}
	nativePending := MigrationProgress{
		Destination:       MigrationDestinationNative,
		CurrentController: MigrationDestinationChurnless,
		Message:           "waiting to start handoff",
	}
	nativeComplete := MigrationProgress{
		Destination:       MigrationDestinationNative,
		CurrentController: MigrationDestinationNative,
		Complete:          true,
		Message:           "handoff complete; native Kubernetes is authoritative",
	}
	interrupted := []string{
		"cluster state may have advanced",
		"run the same command to resume",
	}
	tests := []struct {
		name          string
		engine        *fakeMigrationEngine
		destination   MigrationDestination
		pollInterval  time.Duration
		timeout       time.Duration
		wantSteps     int
		wantObserved  int
		errorContains []string
	}{
		{
			name: "runs to completion and reports progress",
			engine: &fakeMigrationEngine{progress: []MigrationProgress{
				nativePending,
				{
					Destination:       MigrationDestinationNative,
					CurrentController: MigrationDestinationChurnless,
					Mode:              migrationModePreserve,
					Message:           "preserve handoff in progress",
				},
				nativeComplete,
			}},
			destination:  MigrationDestinationNative,
			wantSteps:    2,
			wantObserved: 3,
		},
		{
			name: "immediately drives requested requeues",
			engine: &fakeMigrationEngine{
				progress:   []MigrationProgress{nativePending, nativePending, nativeComplete},
				stepResult: 0,
			},
			destination:  MigrationDestinationNative,
			pollInterval: time.Hour,
			timeout:      500 * time.Millisecond,
			wantSteps:    2,
		},
		{
			name: "is idempotent at the destination",
			engine: &fakeMigrationEngine{progress: []MigrationProgress{{
				Destination:       MigrationDestinationChurnless,
				CurrentController: MigrationDestinationChurnless,
				Complete:          true,
				Message:           "takeover complete; Churnless is authoritative",
			}}},
			destination: MigrationDestinationChurnless,
		},
		{
			name: "returns a terminal advance error",
			engine: &fakeMigrationEngine{
				progress: []MigrationProgress{nativePending},
				stepErr:  errors.New("source Deployment is deleting"),
			},
			destination:   MigrationDestinationNative,
			pollInterval:  time.Hour,
			wantSteps:     1,
			errorContains: []string{"advance migration", "source Deployment is deleting"},
		},
		{
			name: "explains request interruption",
			engine: &fakeMigrationEngine{
				requestErr: context.DeadlineExceeded,
			},
			destination:   MigrationDestinationNative,
			errorContains: interrupted,
		},
		{
			name: "explains advance interruption before progress",
			engine: &fakeMigrationEngine{
				progress:       []MigrationProgress{nativePending},
				advanceErr:     context.DeadlineExceeded,
				advanceErrorAt: 1,
			},
			destination:   MigrationDestinationNative,
			errorContains: interrupted,
		},
		{
			name: "retries a transient initial advance",
			engine: &fakeMigrationEngine{
				progress:       []MigrationProgress{nativePending, nativeComplete},
				advanceErr:     apierrors.NewTooManyRequests("try again", 0),
				advanceErrorAt: 1,
			},
			destination:  MigrationDestinationNative,
			pollInterval: time.Millisecond,
			wantSteps:    1,
		},
		{
			name: "honors a positive advance delay",
			engine: &fakeMigrationEngine{
				progress:   []MigrationProgress{nativePending, nativeComplete},
				stepResult: MigrationStep(time.Hour),
			},
			destination:   MigrationDestinationNative,
			timeout:       50 * time.Millisecond,
			wantSteps:     1,
			errorContains: interrupted,
		},
		{
			name: "explains step interruption",
			engine: &fakeMigrationEngine{
				progress: []MigrationProgress{nativePending},
				stepErr:  context.DeadlineExceeded,
			},
			destination:   MigrationDestinationNative,
			wantSteps:     1,
			errorContains: interrupted,
		},
		{
			name: "stops when superseded",
			engine: &fakeMigrationEngine{progress: []MigrationProgress{
				nativePending,
				{
					Destination:         MigrationDestinationNative,
					RequestedController: MigrationDestinationChurnless,
					CurrentController:   MigrationDestinationChurnless,
					Superseded:          true,
					Message:             "request to native was superseded",
				},
			}},
			destination:   MigrationDestinationNative,
			wantSteps:     1,
			errorContains: []string{"superseded"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			if test.timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, test.timeout)
				defer cancel()
			}
			var observed []MigrationProgress
			err := (&MigrationDriver{
				Engine:       test.engine,
				PollInterval: test.pollInterval,
				Observe:      func(progress MigrationProgress) { observed = append(observed, progress) },
			}).Transfer(ctx, key, test.destination)

			if len(test.errorContains) == 0 && err != nil {
				t.Fatal(err)
			}
			for _, text := range test.errorContains {
				if err == nil || !strings.Contains(err.Error(), text) {
					t.Fatalf("error = %v, want substring %q", err, text)
				}
			}
			if test.engine.requestedKey != key ||
				test.engine.requestedDestination != test.destination {
				t.Fatalf(
					"request = %v to %q, want %v to %q",
					test.engine.requestedKey,
					test.engine.requestedDestination,
					key,
					test.destination,
				)
			}
			if test.engine.steps != test.wantSteps {
				t.Fatalf("engine steps = %d, want %d", test.engine.steps, test.wantSteps)
			}
			if test.wantObserved > 0 &&
				(len(observed) != test.wantObserved || !observed[len(observed)-1].Complete) {
				t.Fatalf("observed progress = %#v", observed)
			}
		})
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
