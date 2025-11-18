import time
import multiprocessing
import random
import os
import signal
import uuid
import threading

# --- Experiment Configuration ---
MEMORY_SIZE_MB = 128          # Size of the simulated database (RAM)
TOTAL_RUN_TIME_SEC = 25       # Total duration of the experiment
CHECKPOINT_FREQUENCY_SEC = 10 # How often to trigger a checkpoint
COMMIT_CHANCE = 0.90          # 90% success rate, 10% rollback
NUM_WORKER_THREADS = 4        # <--- VARIABLE: Try changing this (1, 2, 4, 8)

# --- File Paths ---
WAL_FILE = 'wal.log'
CHECKPOINT_FILE = 'checkpoint.dat'
CHECKPOINT_TEMP = 'checkpoint.dat.tmp'

# --- Global Shared Resources (Process Level) ---
memory_state = None         # The "Database Cache"
checkpoint_requested = False
master_lock = None          # The "Global Lock" (Mutex)
wal_file_handle = None      # Shared file handle for the log

# --- Signal Handler for Checkpoint Trigger ---
def worker_checkpoint_handler(signum, frame):
    global checkpoint_requested
    # Ideally, we just set a flag. The main thread loop handles the rest.
    print(f"\n[Process {os.getpid()}] Signal received: Checkpoint requested.\n")
    checkpoint_requested = True

# --- Checkpoint Logic (Must be called inside the Lock) ---
def perform_checkpoint_locked():
    """
    Persists memory to disk and prunes the WAL.
    CRITICAL: This function assumes the caller already holds 'master_lock'.
    """
    global memory_state, wal_file_handle
    
    print(f"[Thread {threading.current_thread().name}] === Starting Checkpoint (Holding Global Lock) ===")
    start_time = time.time()
    
    # 1. Flush Memory to Disk (The Data File)
    #    This simulates writing dirty pages to the main database file.
    #    It includes a heavy fsync() which pauses the system.
    try:
        with open(CHECKPOINT_TEMP, 'wb') as f:
            f.write(memory_state)
            f.flush()
            os.fsync(f.fileno()) # Heavy I/O operation
        
        # Atomic replace
        os.rename(CHECKPOINT_TEMP, CHECKPOINT_FILE)
    except Exception as e:
        print(f"Checkpoint Failed: {e}")
        return

    # 2. Prune the WAL
    #    Since data is safe in checkpoint.dat, we can clear the log.
    #    We close and reopen to safely truncate.
    wal_file_handle.close()
    with open(WAL_FILE, 'wb') as f:
        f.truncate(0)
    
    # Reopen in append mode for threads to continue
    wal_file_handle = open(WAL_FILE, 'ab', buffering=0)
    
    end_time = time.time()
    duration = end_time - start_time
    print(f"[Thread {threading.current_thread().name}] === Checkpoint Finished. Pause Duration: {duration:.4f}s ===\n")


# --- Worker Thread Logic ---
def worker_thread_logic():
    """
    Simulates a database worker thread executing transactions.
    It must acquire the Global Lock before doing any I/O.
    """
    global memory_state, checkpoint_requested, master_lock, wal_file_handle
    
    tx_count = 0
    last_report_time = time.time()
    
    while True:
        # ============================================================
        # CRITICAL SECTION START
        # Acquire the Global Lock. All other threads must wait here.
        # This demonstrates the serialization bottleneck.
        # ============================================================
        master_lock.acquire()
        
        try:
            # 1. Check if Checkpoint is needed
            #    Only the thread holding the lock can perform this safely.
            if checkpoint_requested:
                perform_checkpoint_locked()
                checkpoint_requested = False # Reset flag

            # 2. Simulate a Transaction (Insert/Update)
            tx_id = str(uuid.uuid4())[:8]
            num_ops = random.randint(1, 5) # 1 to 5 operations per transaction
            undo_log = [] # Keep track of old values for atomicity
            
            # Perform writes (WAL + Memory)
            for _ in range(num_ops):
                address = random.randint(0, len(memory_state) - 1)
                new_data = random.randint(0, 255)
                old_data = memory_state[address]
                
                # Store Undo info
                undo_log.append((address, old_data))
                
                # Write Redo/Undo log entry
                entry = f"UPDATE:{tx_id}:{address}:{old_data}:{new_data}\n".encode('utf-8')
                wal_file_handle.write(entry)
                
                # Modify Memory (Dirty the cache)
                memory_state[address] = new_data
            
            # 3. Decide Fate: Commit or Rollback
            if random.random() < COMMIT_CHANCE:
                # --- COMMIT PATH ---
                # Write Commit marker
                wal_file_handle.write(f"COMMIT:{tx_id}\n".encode('utf-8'))
                
                # FORCE TO DISK (Group Commit)
                # This is the main I/O cost inside the lock.
                wal_file_handle.fsync()
                tx_count += 1
            else:
                # --- ROLLBACK PATH ---
                # Revert memory changes using Undo Log
                for address, old_data in reversed(undo_log):
                    memory_state[address] = old_data
                
                # Write Rollback marker
                wal_file_handle.write(f"ROLLBACK:{tx_id}\n".encode('utf-8'))
                wal_file_handle.fsync()

        finally:
            # ============================================================
            # CRITICAL SECTION END
            # Release lock so another thread can proceed.
            # ============================================================
            master_lock.release()
        
        # Report Throughput (Done outside the lock to minimize overhead)
        current_time = time.time()
        if current_time - last_report_time >= 1.0:
            tps = tx_count / (current_time - last_report_time)
            print(f"[{threading.current_thread().name}] Throughput: {tps:.2f} TPS")
            tx_count = 0
            last_report_time = current_time

# --- Main Worker Process ---
def worker_process_entry(mem_size_bytes):
    global memory_state, master_lock, wal_file_handle
    
    # 1. Initialize Signal Handler
    signal.signal(signal.SIGUSR1, worker_checkpoint_handler)
    
    # 2. Initialize Shared Resources
    memory_state = bytearray(mem_size_bytes)
    master_lock = threading.Lock() # The Global Lock
    
    # Open WAL file (Shared by all threads)
    wal_file_handle = open(WAL_FILE, 'ab', buffering=0)
    
    print(f"[Process] Starting {NUM_WORKER_THREADS} threads with a GLOBAL LOCK strategy...")

    # 3. Spawn Threads
    threads = []
    for i in range(NUM_WORKER_THREADS):
        t = threading.Thread(target=worker_thread_logic, name=f"Thread-{i+1}")
        t.daemon = True # Threads die when process dies
        t.start()
        threads.append(t)
        
    # 4. Keep Main Process Alive
    #    The threads act as daemons, we just wait for the Manager to kill us.
    while True:
        time.sleep(1)

# --- Recovery System (ARIES-lite) ---
def recover_system(mem_size_bytes):
    print("\n[Recovery] +++ System Recovery Started (ARIES-lite) +++")
    total_start = time.time()
    
    # 1. Load Checkpoint (Fast Forward)
    try:
        with open(CHECKPOINT_FILE, 'rb') as f:
            recovered_memory = bytearray(f.read())
        print(f"[Recovery] Phase 1: Checkpoint loaded ({len(recovered_memory)/(1024*1024):.2f} MB)")
    except FileNotFoundError:
        print("[Recovery] Phase 1: No Checkpoint found. Starting from empty state.")
        recovered_memory = bytearray(mem_size_bytes)

    # 2. Analysis Phase
    print(f"[Recovery] Phase 2: Analyzing WAL ({WAL_FILE})...")
    tx_status = {}
    wal_lines = []
    
    if os.path.exists(WAL_FILE):
        with open(WAL_FILE, 'rb') as f:
            for line_bytes in f:
                wal_lines.append(line_bytes)
                try:
                    line = line_bytes.decode('utf-8').strip()
                    parts = line.split(':')
                    if parts[0] == "UPDATE":
                        if parts[1] not in tx_status: tx_status[parts[1]] = "inflight"
                    elif parts[0] == "COMMIT":
                        tx_status[parts[1]] = "committed"
                    elif parts[0] == "ROLLBACK":
                        tx_status[parts[1]] = "aborted"
                except: pass

    inflight = {tx for tx, status in tx_status.items() if status == "inflight"}
    print(f"[Recovery] Analysis complete. Found {len(inflight)} inflight transactions.")

    # 3. Redo Phase (Replay All Updates)
    print("[Recovery] Phase 3: Redo (Replaying History)...")
    redo_count = 0
    for line_bytes in wal_lines:
        try:
            parts = line_bytes.decode('utf-8').strip().split(':')
            if parts[0] == "UPDATE":
                # UPDATE:tx_id:addr:old:new
                addr, new_val = int(parts[2]), int(parts[4])
                recovered_memory[addr] = new_val
                redo_count += 1
        except: pass
    
    # 4. Undo Phase (Revert Inflight Transactions)
    print("[Recovery] Phase 4: Undo (Reverting Inflight Data)...")
    undo_count = 0
    for line_bytes in reversed(wal_lines):
        if not inflight: break
        try:
            parts = line_bytes.decode('utf-8').strip().split(':')
            if parts[0] == "UPDATE":
                tx_id = parts[1]
                if tx_id in inflight:
                    # Revert to OLD value
                    addr, old_val = int(parts[2]), int(parts[3])
                    recovered_memory[addr] = old_val
                    undo_count += 1
        except: pass

    total_end = time.time()
    print(f"\n[Recovery] Recovery Complete.")
    print(f"  - Redo Operations: {redo_count}")
    print(f"  - Undo Operations: {undo_count}")
    print(f"  - Total Time:      {total_end - total_start:.4f} sec")
    print("[Recovery] System is now consistent.\n")


# --- Manager Process ---
if __name__ == "__main__":
    # Cleanup
    for f in [WAL_FILE, CHECKPOINT_FILE, CHECKPOINT_TEMP]:
        if os.path.exists(f): os.remove(f)

    print(f"[Manager] Experiment: Model 3.1 (Hybrid + {NUM_WORKER_THREADS} Threads + Global Lock)")
    
    mem_size = MEMORY_SIZE_MB * 1024 * 1024
    p = multiprocessing.Process(target=worker_process_entry, args=(mem_size,))
    p.start()
    
    start_time = time.time()
    last_checkpoint = start_time
    
    try:
        while time.time() - start_time < TOTAL_RUN_TIME_SEC:
            time.sleep(1)
            
            # Trigger Checkpoint
            if time.time() - last_checkpoint >= CHECKPOINT_FREQUENCY_SEC:
                print("\n[Manager] Triggering Checkpoint Signal...")
                os.kill(p.pid, signal.SIGUSR1)
                last_checkpoint = time.time()
                
    except KeyboardInterrupt:
        print("[Manager] Interrupted.")
        
    finally:
        print(f"\n[Manager] Time's up ({TOTAL_RUN_TIME_SEC}s). Simulating CRASH (kill -9)...")
        p.kill() # Force Kill
        p.join()
        print("[Manager] Process crashed.")
        
        # Check storage
        print("\n--- Storage Stats ---")
        if os.path.exists(WAL_FILE):
            print(f"  WAL Size:        {os.path.getsize(WAL_FILE)/1024:.2f} KB")
        if os.path.exists(CHECKPOINT_FILE):
            print(f"  Checkpoint Size: {os.path.getsize(CHECKPOINT_FILE)/(1024*1024):.2f} MB")
            
        # Run Recovery
        recover_system(mem_size)