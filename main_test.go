package main

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestGeneratePath(t *testing.T) {
	want := fmt.Sprintf("%s%s%s%s", string(filepath.Separator), "first", string(filepath.Separator), "second")
	generatedPath := generatePath("/first", "second")

	if generatedPath != want {
		t.Fatalf("Expected %s, got %s", want, generatedPath)
	}
}

func TestRepoCommitCount(t *testing.T) {
	testRepo := &Repo{
		Id:   0,
		Name: "TestRepo",
		Events: []*Event{
			&Event{
				Id:      0,
				Type:    "",
				ActorId: 0,
				RepoId:  0,
				Commits: []*Commit{
					&Commit{
						Sha:     "x1",
						Message: "Test",
						EventId: 0,
					},
				},
			},
			&Event{
				Id:      1,
				Type:    "",
				ActorId: 0,
				RepoId:  0,
				Commits: []*Commit{
					&Commit{
						Sha:     "x2",
						Message: "Test",
						EventId: 0,
					},
					&Commit{
						Sha:     "x3",
						Message: "Test",
						EventId: 0,
					},
				},
			},
		},
	}

	want := 3
	got := testRepo.countCommits()

	if got != want {
		t.Fatalf("Expected %d, got %d", want, got)
	}
}
