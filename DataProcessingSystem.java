import java.util.LinkedList;
import java.util.Queue;

public class DataProcessingSystem {

    // Shared Resource Queue
    static class TaskQueue {
        private final Queue<String> tasks = new LinkedList<>();

        public synchronized void addTask(String task) {
            tasks.add(task);
            notifyAll();
        }

        public synchronized String getTask() throws InterruptedException {
            while (tasks.isEmpty()) {
                wait();
            }
            return tasks.poll();
        }
    }

    // Shared Results List
    static class ResultList {
        private final Queue<String> results = new LinkedList<>();

        public synchronized void addResult(String result) {
            results.add(result);
        }

        public synchronized void displayResults() {
            System.out.println("\n========== PROCESSED RESULTS ==========");

            for (String result : results) {
                System.out.println(result);
            }

            System.out.println("========================================");
        }
    }

    // Worker Thread
    static class Worker extends Thread {

        private final int workerId;
        private final TaskQueue taskQueue;
        private final ResultList resultList;

        public Worker(
                int workerId,
                TaskQueue taskQueue,
                ResultList resultList) {

            this.workerId = workerId;
            this.taskQueue = taskQueue;
            this.resultList = resultList;
        }

        @Override
        public void run() {

            System.out.println(
                    "Worker " + workerId + " started.");

            while (true) {

                try {

                    // Retrieve a task from the shared queue
                    String task = taskQueue.getTask();

                    // Stop the worker when STOP is received
                    if (task.equals("STOP")) {

                        System.out.println(
                                "Worker " + workerId +
                                " received STOP signal.");

                        break;
                    }

                    System.out.println(
                            "Worker " + workerId +
                            " processing: " + task);

                    // Simulate processing time
                    Thread.sleep(1000);

                    // Create processing result
                    String result =
                            "Worker " + workerId +
                            " completed: " + task;

                    // Save result safely
                    resultList.addResult(result);

                    System.out.println(result);

                } catch (InterruptedException e) {

                    System.err.println(
                            "Worker " + workerId +
                            " interrupted.");

                    Thread.currentThread().interrupt();
                    break;

                } catch (Exception e) {

                    System.err.println(
                            "Worker " + workerId +
                            " encountered an error: " +
                            e.getMessage());

                }
            }

            System.out.println(
                    "Worker " + workerId + " completed.");
        }
    }

    // Main Program
    public static void main(String[] args) {

        System.out.println(
                "========================================");

        System.out.println(
                " MULTI-THREADED DATA PROCESSING SYSTEM");

        System.out.println(
                "========================================");

        // Create shared resources
        TaskQueue taskQueue = new TaskQueue();
        ResultList resultList = new ResultList();

        // Number of workers
        int numberOfWorkers = 3;

        // Add tasks to the shared queue
        for (int i = 1; i <= 10; i++) {

            String task = "Task-" + i;

            taskQueue.addTask(task);

            System.out.println(
                    "Task added: " + task);
        }

        // Create worker threads
        Worker[] workers =
                new Worker[numberOfWorkers];

        for (int i = 0; i < numberOfWorkers; i++) {

            workers[i] = new Worker(
                    i + 1,
                    taskQueue,
                    resultList);

            workers[i].start();
        }

        // Add one STOP signal for every worker
        for (int i = 0; i < numberOfWorkers; i++) {

            taskQueue.addTask("STOP");
        }

        // Wait for all workers to finish
        try {

            for (int i = 0;
                 i < numberOfWorkers;
                 i++) {

                workers[i].join();
            }

        } catch (InterruptedException e) {

            System.err.println(
                    "Main thread interrupted.");

            Thread.currentThread().interrupt();
        }

        // Display final results
        resultList.displayResults();

        System.out.println(
                "\nAll processing completed.");

        System.out.println(
                "Program terminated successfully.");
    }
}