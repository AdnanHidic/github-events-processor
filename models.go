package main

import "strings"

type Event struct {
	Id      int64
	Type    string
	ActorId int64
	RepoId  int64

	Commits []*Commit
}

func (e *Event) isWatchEvent() bool {
	return e.Type == "WatchEvent"
}

func (e *Event) isPullRequestEvent() bool {
	return e.Type == "PullRequestEvent"
}

func (e *Event) commitCount() int {
	return len(e.Commits)
}

type Repo struct {
	Id   int64
	Name string

	Events []*Event
}

func (r *Repo) countEventsWhere(fn func(*Event) bool) int {
	count := 0
	for _, event := range r.Events {
		if fn(event) {
			count++
		}
	}
	return count
}

func (r *Repo) countCommits() int {
	commitCount := 0
	for _, event := range r.Events {
		commitCount += event.commitCount()
	}
	return commitCount
}

type Commit struct {
	Sha     string
	Message string
	EventId int64
}

type Actor struct {
	Id       int64
	Username string

	Events []*Event
}

// don't do this in production, anyone can have bot in their name, but I want pretty results :D
func (a *Actor) isActiveUser() bool {
	return !strings.HasSuffix(a.Username, "[bot]") && !strings.HasSuffix(a.Username, "-bot") && !strings.HasSuffix(a.Username, "Bot") && !strings.Contains(a.Username, "-bot-")
}

func (a *Actor) countCommits() int {
	commitCount := 0
	for _, event := range a.Events {
		commitCount += event.commitCount()
	}
	return commitCount
}

func (a *Actor) countEventsWhere(fn func(*Event) bool) int {
	count := 0
	for _, event := range a.Events {
		if fn(event) {
			count++
		}
	}
	return count
}
