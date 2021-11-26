# github-events-processor

### System requirements
- golang 1.17.3
- GOROOT/GOPATH set up and `go` command available

### How to run
1. Download this repository
2. Navigate to the repository root
3. Execute the following command in terminal
```bash
go run main.go database.go models.go helpers.go --data-path=/path/to/github-events-processor/data
```

### How to run tests
1. Navigate to the repository root
2. Execute the following command in terminal
```bash
go test *.go
```
