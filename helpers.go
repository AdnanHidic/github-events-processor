package main

import "path/filepath"

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
