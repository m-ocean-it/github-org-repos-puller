package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	apiAddress            = "https://api.github.com"
	reposEndpointTemplate = "/orgs/%s/repos"
)

func main() {
	tickPeriod := time.Hour // TODO(mmotyshen): get from config.
	log.Printf("Using an interval of %s", tickPeriod)

	run()
	for range time.Tick(tickPeriod) {
		run()
		log.Printf("Sleeping for %s", tickPeriod)
	}
}

func run() {
	organizationName := os.Getenv("ORGANIZATION_NAME")
	if organizationName == "" {
		log.Fatal("Organization name unknown") // TODO(mmotyshen): hint to solution.
	}

	bearerToken := os.Getenv("ACCESS_TOKEN")
	if bearerToken == "" {
		log.Fatal("Access token unknown") // TODO(mmotyshen): hint to solution.
	}

	storageLocation := os.Getenv("LOCAL_REPOSITORIES_DIR")
	if storageLocation == "" {
		log.Fatal("Local repositories directory unknown") // TODO(mmotyshen): hint to solution.
	}

	ignoreListRaw := os.Getenv("IGNORE_LIST")
	var ignoreList []string
	if ignoreListRaw != "" {
		for ignoreEntryRaw := range strings.SplitSeq(ignoreListRaw, ",") {
			ignoreEntry := strings.TrimSpace(ignoreEntryRaw)
			if ignoreEntry != "" {
				ignoreList = append(ignoreList, ignoreEntry)
			}
		}
	}
	if len(ignoreList) > 0 {
		log.Printf("Ignore-list contains %d entries: %s", len(ignoreList), strings.Join(ignoreList, ", "))
	}

	fullURL, err := url.JoinPath(apiAddress, fmt.Sprintf(reposEndpointTemplate, organizationName))
	if err != nil {
		log.Fatalf("Could not create an API URL for fetching organization repositories list: %v", err)
	}

	req, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		log.Fatalf("Could not create a request for organization repositories list: %v", err)
	}

	req.Header.Add("Accept", "application/vnd.github+json")
	req.Header.Add("Authorization", "Bearer "+bearerToken)
	req.Header.Add("X-GitHub-Api-Version", "2026-03-10")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("Could not request organization repositories list: %v", err)
	}
	defer resp.Body.Close()

	var parsedResp SchemaJson
	err = json.NewDecoder(resp.Body).Decode(&parsedResp)
	if err != nil {
		log.Fatalf("Could not JSON-decode a list of organization repositories from API: %v", err)
	}

	log.Printf("Fetched a list of %d repositories", len(parsedResp))

	eg := errgroup.Group{}
	eg.SetLimit(8) // TODO(mmotyshen): get from config.

	var failedRepos []string
	var failedReposMx sync.Mutex

	for _, itm := range parsedResp {
		if slices.Contains(ignoreList, itm.Name) {
			log.Printf("Skipping %q because it is in the ignore-list", itm.Name)

			continue
		}

		eg.Go(func() error {
			parsedURL, err := url.Parse(itm.HtmlUrl)
			if err != nil {
				log.Fatalf("Could not parse a URL from %q: %v", itm.HtmlUrl, err)
			}

			parsedURL.User = url.UserPassword("oauth2", bearerToken)

			augmentedURL := parsedURL.String()

			localRepoPath := filepath.Join(storageLocation, itm.Name)
			localRepoPathExists, err := pathExists(localRepoPath)
			if err != nil {
				log.Fatalf("Could not check if file path %q exists: %v", localRepoPath, err)
			}

			var cmd *exec.Cmd
			var operation string

			if localRepoPathExists {
				cmd = exec.Command("git", "-C", localRepoPath, "pull")
				operation = "git pull"
			} else {
				cmd = exec.Command("git", "clone", augmentedURL, localRepoPath)
				operation = "git clone"
			}

			_, err = cmd.Output()
			if err != nil {
				errMsg := "..."
				if stdErr, ok := err.(*exec.ExitError); ok {
					errMsg = strings.TrimSpace(string(stdErr.Stderr))
				}

				log.Printf("Could not run %q for repository %q: %v (%s)", operation, itm.Name, err, errMsg)

				failedReposMx.Lock()
				failedRepos = append(failedRepos, itm.Name)
				failedReposMx.Unlock()
			} else {
				log.Printf("Successfully performed %q for repository %q", operation, itm.Name)
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		log.Fatalln(err)
	}

	if len(failedRepos) == 0 {
		log.Printf("Cycle finished. All %d repositories cloned/pulled/ignored successfully.", len(parsedResp))
	} else {
		log.Printf("Cycle finished. %d of %d repositories were not processed succesfully:\n  - %s", len(failedRepos), len(parsedResp), strings.Join(failedRepos, ",\n  - "))
	}

}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}

	return false, err
}
