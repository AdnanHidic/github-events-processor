package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
)

var (
	DataFolderPath  = flag.String("data-path", "", "--data-path={full path to directory with github event data}")
	ActorsFileName  = "actors.csv"
	CommitsFileName = "commits.csv"
	EventsFileName  = "events.csv"
	ReposFileName   = "repos.csv"
	Db              *Database
)

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

	log.Printf("Loading database from data-path...")
	Db = NewDatabase(*DataFolderPath)
	Db.LoadFromDataPath()
	log.Printf("Done loading the database!")
}

func main() {
	initialize()

	n := 10

	fmt.Println("")
	fmt.Printf("Displaying top %d active non-bot users with most watch events:\n", n)
	userActivityAnalysisResults := Db.GetActiveUsersWithMostPRsAndCommits(n)
	for _, result := range userActivityAnalysisResults {
		fmt.Printf("%s: %d PRs, %d commits\n", result.User.Username, result.PrCount, result.CommitCount)
	}

	fmt.Println("")
	fmt.Printf("Displaying top %d repositories with most watch events: \n", n)
	watchEventAnalysisResults := Db.GetReposWithMostWatchEvents(n)
	for _, result := range watchEventAnalysisResults {
		fmt.Printf("%s: %d watch events\n", result.Repo.Name, result.Count)
	}

	fmt.Println("")
	fmt.Printf("Displaying top %d repositories with most commits:\n", n)
	commitAnalysisResults := Db.GetReposWithMostCommits(n)
	for _, result := range commitAnalysisResults {
		fmt.Printf("%s: %d commits\n", result.Repo.Name, result.Count)
	}
}
