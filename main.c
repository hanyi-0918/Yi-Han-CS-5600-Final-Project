#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>

/*
 * This structure represents our simulated "process state".
 * In a real application, this could be gigabytes of complex data.
 * In our simulation, it’s just a counter and some mock data.
 */
#define DATA_SIZE 1024 // 1KB of simulated data
struct ProcessState {
    long update_counter;        // How many units of "work" we have completed
    char data[DATA_SIZE];       // Simulated working data
};

// Name of the checkpoint file
const char* CHECKPOINT_FILE = "checkpoint.dat";

// Signal handler for graceful shutdown (e.g., Ctrl+C)
void handle_exit(int sig) {
    printf("\nExit signal captured... shutting down.\n");
    exit(0);
}

/*
 * Function: save_checkpoint
 * Saves the entire process state in memory to disk.
 */
void save_checkpoint(struct ProcessState *state) {
    printf("Saving checkpoint (count: %ld)...\n", state->update_counter);
    
    // O_WRONLY: write-only mode
    // O_CREAT: create the file if it doesn't exist
    // O_TRUNC: truncate the file if it already exists
    // 0644: file permissions
    int fd = open(CHECKPOINT_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) {
        perror("Failed to open checkpoint file");
        return;
    }

    // 1. Write the entire structure to the file
    if (write(fd, state, sizeof(struct ProcessState)) != sizeof(struct ProcessState)) {
        perror("Failed to write checkpoint");
        close(fd);
        return;
    }

    // 2. [Important] Force the data from OS buffer to be flushed to physical disk
    // This simulates persistence.
    fsync(fd); 
    
    close(fd);
    printf("Checkpoint saved successfully.\n");
}

/*
 * Function: load_checkpoint
 * Called at startup to restore process state.
 */
void load_checkpoint(struct ProcessState *state) {
    printf("Attempting to load checkpoint from %s...\n", CHECKPOINT_FILE);

    int fd = open(CHECKPOINT_FILE, O_RDONLY);
    if (fd == -1) {
        // File doesn’t exist — this is a “cold start”
        printf("No checkpoint found. Initializing new state.\n");
        state->update_counter = 0;
        memset(state->data, 0, DATA_SIZE); // Clear data
        return;
    }

    // Try to read the state from file
    if (read(fd, state, sizeof(struct ProcessState)) != sizeof(struct ProcessState)) {
        // File might be corrupted or incomplete
        fprintf(stderr, "Failed to read checkpoint or file corrupted. Exiting.\n");
        close(fd);
        exit(1);
    }
    
    close(fd);
    printf("State restored successfully! Continuing from count %ld.\n", state->update_counter);
}


/*
 * Main function
 */
int main() {
    // Capture Ctrl+C signal for graceful shutdown
    signal(SIGINT, handle_exit);

    struct ProcessState state;

    // === 1. RECOVERY ===
    // At startup, try to load the last checkpoint
    load_checkpoint(&state);

    // === 2. DO WORK ===
    // Infinite loop simulating a long-running process
    while (1) {
        // Simulate doing some "work"
        state.update_counter++;
        state.data[0] = 'A'; // Randomly modify some memory data
        
        printf("Completed work unit #%ld\n", state.update_counter);

        // === 3. CHECKPOINT ===
        // Save a checkpoint every 10 units of work
        if (state.update_counter % 10 == 0) {
            save_checkpoint(&state);
        }
        
        // Slow down the loop so we can observe the process
        sleep(1);
    }

    return 0;
}
