package main

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
)

type Database struct {
	DataPath string
	Events   map[int64]*Event
	Repos    map[int64]*Repo
	Commits  map[string]*Commit
	Actors   map[int64]*Actor
}

func NewDatabase(dataPath string) *Database {
	return &Database{
		DataPath: dataPath,
		Events:   map[int64]*Event{},
		Repos:    map[int64]*Repo{},
		Commits:  map[string]*Commit{},
		Actors:   map[int64]*Actor{},
	}
}

func (db *Database) processCsvFile(filePath string, processFn func([]string)) {
	if f, err := os.Open(filePath); err != nil {
		log.Fatalf("Could not read file %s Error: %s", filePath, err.Error())
	} else {
		defer func(f *os.File) {
			err := f.Close()
			if err != nil {
				log.Printf("Failed to close file %s Error: %s", filePath, err.Error())
			}
		}(f)

		// read csv values using csv.Reader
		csvReader := csv.NewReader(f)
		// Iterate through the records
		for {
			// Read each record from csv
			if record, err := csvReader.Read(); err == io.EOF {
				break
			} else if err != nil {
				log.Fatalf("Reading file %s failed. Error: %s", filePath, err.Error())
			} else {
				processFn(record)
			}
		}
	}
}

func (db *Database) LoadFromDataPath() {
	// load actors
	log.Printf("Started loading data from file %s", ActorsFileName)
	db.processCsvFile(
		generatePath(db.DataPath, ActorsFileName),
		func(record []string) {
			// actor_id, username
			actorId, _ := strconv.ParseInt(record[0], 10, 64)
			db.Actors[actorId] = &Actor{
				Id:       actorId,
				Username: record[1],
			}
		},
	)
	log.Printf("Done loading data from file %s", ActorsFileName)

	// load commits
	log.Printf("Started loading data from file %s", CommitsFileName)
	tmpEventCommits := map[int64][]*Commit{}
	db.processCsvFile(
		generatePath(db.DataPath, CommitsFileName),
		func(record []string) {
			// sha, message, event_id
			eventId, _ := strconv.ParseInt(record[2], 10, 64)
			db.Commits[record[0]] = &Commit{
				Sha:     record[0],
				Message: record[1],
				EventId: eventId,
			}

			// populate the inverse index
			if _, ok := tmpEventCommits[eventId]; ok {
				tmpEventCommits[eventId] = append(tmpEventCommits[eventId], db.Commits[record[0]])
			} else {
				tmpEventCommits[eventId] = []*Commit{db.Commits[record[0]]}
			}
		},
	)
	log.Printf("Done loading data from file %s", CommitsFileName)

	// load events
	log.Printf("Started loading data from file %s", EventsFileName)
	tmpRepoEvents := map[int64][]*Event{}
	tmpActorEvents := map[int64][]*Event{}
	db.processCsvFile(
		generatePath(db.DataPath, EventsFileName),
		func(record []string) {
			// id, type, actor_id, repo_id
			eventId, _ := strconv.ParseInt(record[0], 10, 64)
			actorId, _ := strconv.ParseInt(record[2], 10, 64)
			repoId, _ := strconv.ParseInt(record[3], 10, 64)
			db.Events[eventId] = &Event{
				Id:      eventId,
				Type:    record[1],
				ActorId: actorId,
				RepoId:  repoId,
				Commits: tmpEventCommits[eventId],
			}

			// populate the inverse index
			if _, ok := tmpRepoEvents[repoId]; ok {
				tmpRepoEvents[repoId] = append(tmpRepoEvents[repoId], db.Events[eventId])
			} else {
				tmpRepoEvents[repoId] = []*Event{db.Events[eventId]}
			}
			if _, ok := tmpActorEvents[actorId]; ok {
				tmpActorEvents[actorId] = append(tmpActorEvents[actorId], db.Events[eventId])
			} else {
				tmpActorEvents[actorId] = []*Event{db.Events[eventId]}
			}
		},
	)
	for actorId, _ := range db.Actors {
		db.Actors[actorId].Events = tmpActorEvents[actorId]
	}
	log.Printf("Done loading data from file %s", EventsFileName)

	// load repos
	log.Printf("Started loading data from file %s", ReposFileName)
	db.processCsvFile(
		generatePath(db.DataPath, ReposFileName),
		func(record []string) {
			// id, name
			repoId, _ := strconv.ParseInt(record[0], 10, 64)
			db.Repos[repoId] = &Repo{
				Id:     repoId,
				Name:   record[1],
				Events: tmpRepoEvents[repoId],
			}
		},
	)
	log.Printf("Done loading data from file %s", ReposFileName)
}

type RepoAnalysisItem struct {
	Repo  *Repo
	Count int
}

func (db *Database) GetReposWithMostWatchEvents(n int) []*RepoAnalysisItem {
	topNrepos := []*RepoAnalysisItem{}

	for _, repo := range db.Repos {
		numberOfWatchEvents := repo.countEventsWhere(func(e *Event) bool { return e.isWatchEvent() })

		if len(topNrepos) == 0 || len(topNrepos) < n {
			topNrepos = append(topNrepos, &RepoAnalysisItem{Repo: repo, Count: numberOfWatchEvents})
		} else if topNrepos[len(topNrepos)-1].Count < numberOfWatchEvents {
			topNrepos = topNrepos[:len(topNrepos)-1]
			topNrepos = append(topNrepos, &RepoAnalysisItem{Repo: repo, Count: numberOfWatchEvents})
			sort.Slice(topNrepos, func(i, j int) bool {
				return topNrepos[i].Count > topNrepos[j].Count
			})
		}
	}

	return topNrepos
}

func (db *Database) GetReposWithMostCommits(n int) []*RepoAnalysisItem {
	topNrepos := []*RepoAnalysisItem{}

	for _, repo := range db.Repos {
		numberOfCommits := repo.countCommits()

		if len(topNrepos) == 0 || len(topNrepos) < n {
			topNrepos = append(topNrepos, &RepoAnalysisItem{Repo: repo, Count: numberOfCommits})
		} else if topNrepos[len(topNrepos)-1].Count < numberOfCommits {
			topNrepos = topNrepos[:len(topNrepos)-1]
			topNrepos = append(topNrepos, &RepoAnalysisItem{Repo: repo, Count: numberOfCommits})
			sort.Slice(topNrepos, func(i, j int) bool {
				return topNrepos[i].Count > topNrepos[j].Count
			})
		}
	}

	return topNrepos
}

type UserAnalysisItem struct {
	User        *Actor
	PrCount     int
	CommitCount int
}

func (db *Database) GetActiveUsersWithMostPRsAndCommits(n int) []*UserAnalysisItem {
	topNusers := []*UserAnalysisItem{}

	for _, actor := range db.Actors {
		if actor.isActiveUser() {
			// number of pr events
			numberOfPrEvents := actor.countEventsWhere(func(e *Event) bool { return e.isPullRequestEvent() })
			// number of commits
			numberOfCommits := actor.countCommits()

			if len(topNusers) == 0 || len(topNusers) < n {
				topNusers = append(topNusers, &UserAnalysisItem{User: actor, PrCount: numberOfPrEvents, CommitCount: numberOfCommits})
			} else if topNusers[len(topNusers)-1].PrCount <= numberOfPrEvents {
				topNusers = append(topNusers, &UserAnalysisItem{User: actor, PrCount: numberOfPrEvents, CommitCount: numberOfCommits})
				sort.Slice(topNusers, func(i, j int) bool {
					if topNusers[i].PrCount < topNusers[j].PrCount {
						return false
					} else if topNusers[i].PrCount > topNusers[j].PrCount {
						return true
					} else {
						return topNusers[i].CommitCount > topNusers[j].CommitCount
					}
				})

				topNusers = topNusers[:len(topNusers)-1]

			}
		}
	}

	return topNusers
}
