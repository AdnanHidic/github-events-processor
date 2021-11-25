package main

import (
	"encoding/csv"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

var (
	DataFolderPath  = flag.String("data-path", "", "--data-path={full path to directory with github event data}")
	ActorsFileName  = "actors.csv"
	CommitsFileName = "commits.csv"
	EventsFileName  = "events.csv"
	ReposFileName   = "repos.csv"
)

func generatePath(pathElements ...string) string {
	fullPath := ""
	for i := 0; i < len(pathElements); i++ {
		if i == 0 {
			fullPath += pathElements[i]
		} else {
			fullPath += "/" + pathElements[i]
		}
	}
	return filepath.FromSlash(fullPath)
}

type Event struct {
	Id      int64
	Type    string
	ActorId int64
	RepoId  int64

	Commits []*Commit
}

type Repo struct {
	Id   int64
	Name string

	Events []*Event
}

type Commit struct {
	Sha     string
	Message string
	EventId int64
}

type Actor struct {
	Id       int64
	Username string
}

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

func (db *Database) processCsvFile(filePath string, processFn func([]string)) error {
	if f, err := os.Open(filePath); err != nil {
		log.Fatalf("Could not read file %s Error: %s", filePath, err.Error())
	} else {
		defer f.Close()

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

	return nil
}

func (db *Database) LoadFromDataPath() error {
	// load actors
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

	// load commits
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
			if commits, ok := tmpEventCommits[eventId]; ok {
				commits = append(commits, db.Commits[record[0]])
			} else {
				tmpEventCommits[eventId] = []*Commit{db.Commits[record[0]]}
			}
		},
	)

	// load events
	tmpRepoEvents := map[int64][]*Event{}
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
			if events, ok := tmpRepoEvents[repoId]; ok {
				events = append(events, db.Events[eventId])
			} else {
				tmpRepoEvents[repoId] = []*Event{db.Events[eventId]}
			}
		},
	)

	// load repos
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

	return nil
}

func initialize() {
	flag.Parse()

	log.Printf("Running the analysis on the data-path=%s", *DataFolderPath)

	// confirm data-path is specified and points to properly set up data
	if *DataFolderPath == "" {
		log.Fatalln("Failed to start the analysis: data-path is empty!")
	}

	if files, err := ioutil.ReadDir(*DataFolderPath); err != nil {
		log.Fatalf("Failed to start the analysis: %s", err.Error())
	} else {
		requiredFileNames := [4]string{ActorsFileName, CommitsFileName, EventsFileName, ReposFileName}
		for _, requiredFileName := range requiredFileNames {
			requiredFilePresentInDataFolderPath := false
			for _, file := range files {
				if file.IsDir() || requiredFileName != file.Name() {
					continue
				}
				requiredFilePresentInDataFolderPath = true
				break
			}
			if !requiredFilePresentInDataFolderPath {
				log.Fatalf("Failed to start the analysis: file %s not found in data-path!", requiredFileName)
			} else {
				log.Printf("Found required data file: %s", requiredFileName)
			}
		}
	}

	NewDatabase(*DataFolderPath).LoadFromDataPath()
}

func main() {
	initialize()
}
