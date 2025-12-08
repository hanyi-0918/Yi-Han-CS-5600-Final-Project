import time
import multiprocessing
import random
import os
import signal
import uuid
import threading

# --- Experiment Configuration ---
MEMORY_SIZE_MB = 128          # Simulated database size (RAM)
TOTAL_RUN_TIME_SEC = 25       # Total experiment duration
CHECKPOINT_FREQUENCY_SEC = 5  # Checkpoint trigger frequency
COMMIT_CHANCE = 0.90          # 90% commit, 10% rollback
NUM_WORKER_THREADS = 1        # <--- Adjustable thread count (1, 2, 4, 8)

# --- File Paths ---
WAL_FILE = 'wal.log'
CHECKPOINT_FILE = 'checkpoint.dat'
CHECKPOINT_TEMP = 'checkpoint.dat.tmp'

# --- Global Shared Resources (Process Level) ---
memory_state = None         # In-memory database
checkpoint_requested = False
master_lock = None          # Global Lock
wal_file_handle = None      # Shared WAL file handle

# [Global Stats] For average throughput calculation
total_transactions_global = 0
process_start_time = 0
shared_return_dict = None   # Cross-process shared dictionary

# --- Signal Handler ---
def worker_checkpoint_handler(signum, frame):
    global checkpoint_requested
    checkpoint_requested = True

# --- Checkpoint Logic (Must be called while holding the lock) ---
def perform_checkpoint_locked():
    global memory_state, wal_file_handle
    
    print(f"[Thread {threading.current_thread().name}] === Starting Checkpoint (Holding Global Lock) ===")
    start_time = time.time()
    
    # 1. Flush memory to disk (Data File)
    try:
        with open(CHECKPOINT_TEMP, 'wb') as f:
            f.write(memory_state)
            f.flush()
            os.fsync(f.fileno()) # Force flush to disk
        
        os.rename(CHECKPOINT_TEMP, CHECKPOINT_FILE)
    except Exception as e:
        print(f"Checkpoint Failed: {e}")
        return

    # 2. Prune WAL
    wal_file_handle.close()
    with open(WAL_FILE, 'wb') as f:
        f.truncate(0)
    
    # Reopen in Append mode
    wal_file_handle = open(WAL_FILE, 'ab', buffering=0)
    
    end_time = time.time()
    print(f"[Thread {threading.current_thread().name}] === Checkpoint Finished. Pause: {end_time - start_time:.4f}s ===\n")


# --- Worker Thread Logic ---
def worker_thread_logic():
    global memory_state, checkpoint_requested, master_lock, wal_file_handle
    global total_transactions_global, shared_return_dict, process_start_time
    
    tx_count_local = 0
    last_report_time = time.time()
    
    while True:
        # ============================================================
        # CRITICAL SECTION START
        # Acquire Global Lock, simulating Python GIL or DB Global Lock
        # ============================================================
        master_lock.acquire()
        
        try:
            # 1. Check if Checkpoint is needed
            if checkpoint_requested:
                perform_checkpoint_locked()
                checkpoint_requested = False 

            # 2. Simulate transaction
            tx_id = str(uuid.uuid4())[:8]
            num_ops = random.randint(1, 5) 
            undo_log = [] 
            
            # Write WAL + Modify memory
            for _ in range(num_ops):
                address = random.randint(0, len(memory_state) - 1)
                new_data = random.randint(0, 255)
                old_data = memory_state[address]
                
                undo_log.append((address, old_data))
                entry = f"UPDATE:{tx_id}:{address}:{old_data}:{new_data}\n".encode('utf-8')
                wal_file_handle.write(entry)
                memory_state[address] = new_data
            
            # 3. Commit or Rollback
            if random.random() < COMMIT_CHANCE:
                # --- COMMIT PATH ---
                wal_file_handle.write(f"COMMIT:{tx_id}\n".encode('utf-8'))
                
                # [Modified] Use os.fsync(fd) to ensure physical write to disk
                os.fsync(wal_file_handle.fileno())
                
                tx_count_local += 1
                total_transactions_global += 1
            else:
                # --- ROLLBACK PATH ---
                for address, old_data in reversed(undo_log):
                    memory_state[address] = old_data
                
                wal_file_handle.write(f"ROLLBACK:{tx_id}\n".encode('utf-8'))
                
                # [Modified] Use os.fsync(fd)
                os.fsync(wal_file_handle.fileno())

        finally:
           
            master_lock.release()
        
        # Throughput reporting
        current_time = time.time()
        if current_time - last_report_time >= 1.0:
            tps = tx_count_local / (current_time - last_report_time)
            
            # Update global average TPS
            total_duration = current_time - process_start_time
            avg_tps = total_transactions_global / total_duration if total_duration > 0 else 0
            if shared_return_dict is not None:
                shared_return_dict['avg_tps'] = avg_tps
            
            print(f"[{threading.current_thread().name}] TPS: {tps:.2f}")
            
            tx_count_local = 0
            last_report_time = current_time

# --- Main Worker Process (INITIALIZATION) ---
def worker_process_entry(mem_size_bytes, return_dict):
    global memory_state, master_lock, wal_file_handle
    global shared_return_dict, process_start_time, total_transactions_global
    
    # 1. Initialize Signal Handler
    signal.signal(signal.SIGUSR1, worker_checkpoint_handler)
    
    # 2. Initialize Shared Resources
    memory_state = bytearray(mem_size_bytes)
    master_lock = threading.Lock() # <--- Lock is created here!
    
    # 3. Initialize Stats
    shared_return_dict = return_dict
    process_start_time = time.time()
    total_transactions_global = 0
    
    # 4. Open File (buffering=0: Binary unbuffered mode)
    wal_file_handle = open(WAL_FILE, 'ab', buffering=0)
    
    print(f"[Process] Starting {NUM_WORKER_THREADS} threads with a GLOBAL LOCK strategy...")

    # 5. Spawn Threads
    threads = []
    for i in range(NUM_WORKER_THREADS):
        t = threading.Thread(target=worker_thread_logic, name=f"Thread-{i+1}")
        t.daemon = True 
        t.start()
        threads.append(t)
        
    # 6. Keep Main Process Alive
    while True:
        time.sleep(1)

# --- Recovery System ---
def recover_system(mem_size_bytes):
    print("\n[Recovery] +++ System Recovery Started (ARIES-lite) +++")
    total_start = time.time()
    
    try:
        with open(CHECKPOINT_FILE, 'rb') as f:
            recovered_memory = bytearray(f.read())
        print(f"[Recovery] Phase 1: Checkpoint loaded ({len(recovered_memory)/(1024*1024):.2f} MB)")
    except FileNotFoundError:
        print("[Recovery] Phase 1: No Checkpoint found. Starting from empty.")
        recovered_memory = bytearray(mem_size_bytes)

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

    print("[Recovery] Phase 3: Redo...")
    redo_count = 0
    for line_bytes in wal_lines:
        try:
            parts = line_bytes.decode('utf-8').strip().split(':')
            if parts[0] == "UPDATE":
                addr, new_val = int(parts[2]), int(parts[4])
                recovered_memory[addr] = new_val
                redo_count += 1
        except: pass
    
    print("[Recovery] Phase 4: Undo...")
    undo_count = 0
    for line_bytes in reversed(wal_lines):
        if not inflight: break
        try:
            parts = line_bytes.decode('utf-8').strip().split(':')
            if parts[0] == "UPDATE":
                tx_id = parts[1]
                if tx_id in inflight:
                    addr, old_val = int(parts[2]), int(parts[3])
                    recovered_memory[addr] = old_val
                    undo_count += 1
        except: pass

    print(f"\n[Recovery] Recovery Complete.")
    print(f"  - Redo Operations: {redo_count}")
    print(f"  - Undo Operations: {undo_count}")
    print(f"  - Total Time:      {time.time() - total_start:.4f} sec")


# --- Manager Process (MAIN EXECUTION) ---
if __name__ == "__main__":
    # Cleanup old files
    for f in [WAL_FILE, CHECKPOINT_FILE, CHECKPOINT_TEMP]:
        if os.path.exists(f): os.remove(f)

    print(f"[Manager] Experiment: Model 3.1 (Hybrid + {NUM_WORKER_THREADS} Threads + Global Lock)")
    
    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    
    mem_size = MEMORY_SIZE_MB * 1024 * 1024
    
    # Start the worker process
    p = multiprocessing.Process(target=worker_process_entry, args=(mem_size, return_dict))
    p.start()
    
    start_time = time.time()
    last_checkpoint = start_time
    
    try:
        while time.time() - start_time < TOTAL_RUN_TIME_SEC:
            time.sleep(1)
            # Trigger Checkpoint signal periodically
            if time.time() - last_checkpoint >= CHECKPOINT_FREQUENCY_SEC:
                # print("[Manager] Triggering Checkpoint...")
                os.kill(p.pid, signal.SIGUSR1)
                last_checkpoint = time.time()
                
    except KeyboardInterrupt:
        print("[Manager] Interrupted.")
        
    finally:
        print(f"\n[Manager] Time's up. Simulating CRASH...")
        p.kill() # Force Kill to simulate crash
        p.join()
        
        avg_tps = return_dict.get('avg_tps', 0)
        
        print("\n--- Final Statistics ---")
        print(f"  System Average Throughput: {avg_tps:.2f} TPS")
        
        print("\n--- Storage Stats ---")
        if os.path.exists(WAL_FILE):
            print(f"  WAL Size:        {os.path.getsize(WAL_FILE)/1024:.2f} KB")
        if os.path.exists(CHECKPOINT_FILE):
            print(f"  Checkpoint Size: {os.path.getsize(CHECKPOINT_FILE)/(1024*1024):.2f} MB")
            
        # Run recovery
        recover_system(mem_size)
