package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	apiAddress            = "https://api.github.com"
	reposEndpointTemplate = "/orgs/%s/repos"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	interval := time.Hour // TODO(mmotyshen): get from config.
	log.Printf("Using an interval of %s between runs", interval)

	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			log.Printf("Breaking from the main loop due to context cancellation: %s", ctxErr)

			break
		}

		log.Printf("Running the cycle")

		err := run(ctx)
		if err != nil {
			log.Printf("Cycle errored: %s", err)
		} else {
			log.Printf("Cycle completed successfully")
		}

		log.Printf("Sleeping for %s", interval)
		select {
		case <-ctx.Done():
			continue
		case <-time.After(interval):
		}
	}

	log.Print("Process exits gracefully")
}

func run(ctx context.Context) error {
	organizationName := os.Getenv("ORGANIZATION_NAME")
	if organizationName == "" {
		return fmt.Errorf("Organization name unknown") // TODO(mmotyshen): hint to solution.
	}

	bearerToken := os.Getenv("ACCESS_TOKEN")
	if bearerToken == "" {
		return fmt.Errorf("Access token unknown") // TODO(mmotyshen): hint to solution.
	}

	storageLocation := os.Getenv("LOCAL_REPOSITORIES_DIR")
	if storageLocation == "" {
		return fmt.Errorf("Local repositories directory unknown") // TODO(mmotyshen): hint to solution.
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

	successPingURL := os.Getenv("SUCCESS_PING_URL")
	if successPingURL != "" {
		log.Printf("Success will be pinged to %q", successPingURL)
	}

	fullURL, err := url.JoinPath(apiAddress, fmt.Sprintf(reposEndpointTemplate, organizationName))
	if err != nil {
		return fmt.Errorf("Could not create an API URL for fetching organization repositories list: %v", err)
	}

	var entries ResponseEntries
	for page := 1; ; page++ {
		req, err := http.NewRequest("GET", fullURL, nil)
		if err != nil {
			return fmt.Errorf("Could not create a request for organization repositories list: %v", err)
		}

		query := req.URL.Query()
		query.Add("page", strconv.Itoa(page))
		req.URL.RawQuery = query.Encode()

		req.Header.Add("Accept", "application/vnd.github+json")
		req.Header.Add("Authorization", "Bearer "+bearerToken)
		req.Header.Add("X-GitHub-Api-Version", "2026-03-10")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("Could not request organization repositories list: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return fmt.Errorf("received a non-OK status-code from API (%d, %s)",
				resp.StatusCode, http.StatusText(resp.StatusCode))
		}

		var respEntries ResponseEntries
		err = json.NewDecoder(resp.Body).Decode(&respEntries)
		if err != nil {
			return fmt.Errorf("Could not JSON-decode a list of organization repositories from API: %v", err)
		}

		resp.Body.Close()
		log.Printf("Received a list of %d repositories", len(respEntries))

		if len(respEntries) == 0 {
			log.Printf("Last page read")

			break
		}

		entries = append(entries, respEntries...)
	}

	rand.Shuffle(len(entries), func(a, b int) {
		entries[a], entries[b] = entries[b], entries[a]
	})

	log.Printf("Fetched info of %d repositories in total", len(entries))

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(8) // TODO(mmotyshen): get from config.

	var failedRepos []string
	var failedReposMx sync.Mutex

	for _, itm := range entries {
		if slices.Contains(ignoreList, itm.Name) {
			log.Printf("Skipping %q because it is in the ignore-list", itm.Name)

			continue
		}

		eg.Go(func() error {
			ctx := egCtx
			ctx, cancel := context.WithTimeout(ctx, time.Minute*30)
			defer cancel()

			parsedURL, err := url.Parse(itm.HtmlUrl)
			if err != nil {
				return fmt.Errorf("Could not parse a URL from %q: %v", itm.HtmlUrl, err)
			}

			parsedURL.User = url.UserPassword("oauth2", bearerToken)

			augmentedURL := parsedURL.String()

			localRepoPath := filepath.Join(storageLocation, itm.Name)
			localRepoPathExists, err := pathExists(localRepoPath)
			if err != nil {
				return fmt.Errorf("Could not check if file path %q exists: %v", localRepoPath, err)
			}

			var operation string

			if localRepoPathExists {
				err = runGitFetchReset(ctx, localRepoPath)
				operation = "git fetch+reset"
			} else {
				err = runGitClone(ctx, augmentedURL, localRepoPath)
				operation = "git clone"
			}

			if err != nil {
				log.Printf("Could not run %q for repository %q: %s", operation, itm.Name, err)

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
		return err
	}

	if len(failedRepos) == 0 {
		log.Printf("Cycle finished. All %d repositories cloned/pulled/ignored successfully.", len(entries))

		if successPingURL != "" {
			resp, err := http.Get(successPingURL)
			if err != nil {
				log.Printf("Pinging about success failed: %s", err)
			} else {
				resp.Body.Close()
			}
		}
	} else {
		log.Printf("Cycle finished. %d of %d repositories were not processed succesfully:\n  - %s", len(failedRepos), len(entries), strings.Join(failedRepos, ",\n  - "))
	}

	return nil
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

func runGitFetchReset(ctx context.Context, repoPath string) error {
	branch, err := runCmd(ctx, runCmdSpec{
		dir:  repoPath,
		name: "git",
		args: []string{"rev-parse", "--abbrev-ref", "HEAD"},
	})
	if err != nil {
		return fmt.Errorf("resolve current branch: %w", err)
	}
	branch = strings.TrimSpace(branch)

	_, err = runCmd(ctx, runCmdSpec{
		dir:  repoPath,
		name: "git",
		args: []string{"fetch"},
	})
	if err != nil {
		return fmt.Errorf("git fetch: %w", err)
	}

	_, err = runCmd(ctx, runCmdSpec{
		dir:  repoPath,
		name: "git",
		args: []string{"reset", "--hard", "origin/" + branch},
	})
	if err != nil {
		return fmt.Errorf("git reset: %w", err)
	}

	return nil
}

func runGitClone(ctx context.Context, url string, destination string) error {
	_, err := runCmd(ctx, runCmdSpec{
		name: "git",
		args: []string{"clone", url, destination},
	})

	if err != nil {
		return fmt.Errorf("git clone: %w", err)
	}

	return nil
}

type runCmdSpec struct {
	dir  string
	name string
	args []string
}

func runCmd(ctx context.Context, spec runCmdSpec) (string, error) {
	cmd := exec.CommandContext(ctx, spec.name, spec.args...)
	cmd.Dir = spec.dir

	out, err := cmd.Output()
	if err != nil {
		errMsg := "..."
		if stdErr, ok := err.(*exec.ExitError); ok {
			errMsg = strings.TrimSpace(string(stdErr.Stderr))
		}

		return "", fmt.Errorf("Could not run command %q: %s (%s)", cmd.String(), err, errMsg)
	}

	return string(out), nil
}
