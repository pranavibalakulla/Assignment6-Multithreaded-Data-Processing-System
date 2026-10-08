package main

import (
	"fmt"
	"sync"
	"time"
)

// Shared task queue using a Go channel
type TaskQueue struct {
	tasks chan string
}

// Shared results structure
type ResultList struct {
	mu      sync.Mutex
	results []string
}

// Add a result safely
func (r *ResultList) addResult(result string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.results = append(r.results, result)
}

// Display all processed results
func (r *ResultList) displayResults() {
	r.mu.Lock()
	defer r.mu.Unlock()

	fmt.Println("\n========== PROCESSED RESULTS ==========")

	for _, result := range r.results {
		fmt.Println(result)
	}

	fmt.Println("========================================")
}

// Worker function
func worker(
	workerID int,
	taskQueue <-chan string,
	results *ResultList,
	wg *sync.WaitGroup,
) {

	defer wg.Done()

	fmt.Printf("Worker %d started.\n", workerID)

	for task := range taskQueue {

		fmt.Printf(
			"Worker %d processing: %s\n",
			workerID,
			task,
		)

		// Simulate computational work
		time.Sleep(1 * time.Second)

		result := fmt.Sprintf(
			"Worker %d completed: %s",
			workerID,
			task,
		)

		// Store result safely
		results.addResult(result)

		fmt.Println(result)
	}

	fmt.Printf(
		"Worker %d completed.\n",
		workerID,
	)
}

func main() {

	fmt.Println("========================================")
	fmt.Println(" MULTI-THREADED DATA PROCESSING SYSTEM")
	fmt.Println("========================================")

	// Create shared task channel
	taskQueue := make(chan string)

	// Create shared results list
	results := &ResultList{
		results: make([]string, 0),
	}

	// Number of workers
	numberOfWorkers := 3

	// WaitGroup for worker synchronization
	var wg sync.WaitGroup

	wg.Add(numberOfWorkers)

	// Start worker goroutines
	for i := 1; i <= numberOfWorkers; i++ {

		go worker(
			i,
			taskQueue,
			results,
			&wg,
		)
	}

	// Send tasks to the shared channel
	go func() {

		for i := 1; i <= 10; i++ {

			task := fmt.Sprintf(
				"Task-%d",
				i,
			)

			fmt.Printf(
				"Task added: %s\n",
				task,
			)

			taskQueue <- task
		}

		// Close the channel after all tasks are sent
		close(taskQueue)
	}()

	// Wait for all workers to finish
	wg.Wait()

	// Display final results
	results.displayResults()

	fmt.Println("\nAll processing completed.")
	fmt.Println("Program terminated successfully.")
}