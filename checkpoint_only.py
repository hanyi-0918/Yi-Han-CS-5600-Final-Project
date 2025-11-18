import time
import multiprocessing
import random
import os
import signal
import uuid

# --- Configuration ---
MEMORY_SIZE_MB = 128
TOTAL_RUN_TIME_SEC = 25
CHECKPOINT_FREQUENCY_SEC = 10

# --- Files ---
# No WAL_FILE in this model
CHECKPOINT_FILE = 'checkpoint.dat'
CHECKPOINT_TEMP = 'checkpoint.dat.tmp'

# --- Global Resources ---
memory_state = None
checkpoint_requested = False

# --- Checkpoint Logic ---
def worker_checkpoint_handler(signum, frame):
    global checkpoint_requested
    print(f"\n[Worker] Signal received: Checkpoint requested.\n")
    checkpoint_requested = True

def perform_checkpoint():
    global checkpoint_requested, memory_state
    print(f"[Worker] === Starting Checkpoint ===")
    start = time.time()
    
    # Flush entire memory to disk
    try:
        with open(CHECKPOINT_TEMP, 'wb') as f:
            f.write(memory_state)
            f.flush()
            os.fsync(f.fileno())
        os.rename(CHECKPOINT_TEMP, CHECKPOINT_FILE)
    except Exception as e:
        print(f"Checkpoint Error: {e}")
        return
    
    end = time.time()
    print(f"[Worker] === Checkpoint Finished. Pause: {end - start:.4f}s ===\n")
    checkpoint_requested = False

# --- Worker Logic ---
def worker_process(mem_size_bytes):
    global memory_state, checkpoint_requested
    
    signal.signal(signal.SIGUSR1, worker_checkpoint_handler)
    memory_state = bytearray(mem_size_bytes)
    print(f"[Worker PID: {os.getpid()}] Starting Model 2 (Checkpoint-Only)...")

    transaction_count = 0
    last_report_time = time.time()

    try:
        while True:
            # Checkpoint trigger
            if checkpoint_requested:
                perform_checkpoint()
            
            # Simulate Transaction
            # Note: WITHOUT fsync, this loop runs extremely fast (CPU bound)
            num_ops = random.randint(1, 5)
            for _ in range(num_ops):
                address = random.randint(0, len(memory_state) - 1)
                val = random.randint(0, 255)
                memory_state[address] = val
            
            transaction_count += 1
            
            # Throughput Reporting
            current_time = time.time()
            if current_time - last_report_time >= 1.0:
                tps = transaction_count / (current_time - last_report_time)
                print(f"[Worker] Throughput: {tps:.2f} TPS")
                transaction_count = 0
                last_report_time = current_time

    except KeyboardInterrupt:
        pass
    finally:
        print(f"[Worker] Exiting.")

# --- Recovery System (Simple Reload) ---
def recover_system(mem_size_bytes):
    print("\n[Recovery] Starting Recovery (Model 2: Checkpoint-Only)...")
    total_start = time.time()
    
    # 1. Load Checkpoint
    cp_load_start = time.time()
    try:
        with open(CHECKPOINT_FILE, 'rb') as f:
            recovered_memory = bytearray(f.read())
        print(f"[Recovery] Checkpoint loaded ({len(recovered_memory)/(1024*1024):.2f} MB)")
    except FileNotFoundError:
        print("[Recovery] No Checkpoint found. Data is completely lost.")
        recovered_memory = bytearray(mem_size_bytes)
    cp_load_end = time.time()

    # 2. No WAL to replay
    print(f"[Recovery] No WAL file exists.")
    print(f"[Recovery] CRITICAL: All transactions since the last checkpoint are LOST.")
    
    total_end = time.time()
    print(f"\n[Recovery] Statistics:")
    print(f"  - Checkpoint Load Time: {cp_load_end - cp_load_start:.4f}s")
    print(f"  - WAL Replay Time: 0.0000s")
    print(f"  - Total Recovery Time: {total_end - total_start:.4f}s")

# --- Manager ---
if __name__ == "__main__":
    for f in [CHECKPOINT_FILE, CHECKPOINT_TEMP]:
        if os.path.exists(f): os.remove(f)

    print(f"[Manager] Experiment: Model 2 (Checkpoint-Only)")
    mem_size = MEMORY_SIZE_MB * 1024 * 1024
    
    p = multiprocessing.Process(target=worker_process, args=(mem_size,))
    p.start()
    
    start_time = time.time()
    last_cp = start_time
    
    try:
        while time.time() - start_time < TOTAL_RUN_TIME_SEC:
            time.sleep(1)
            if time.time() - last_cp >= CHECKPOINT_FREQUENCY_SEC:
                print("\n[Manager] Triggering Checkpoint...")
                os.kill(p.pid, signal.SIGUSR1)
                last_cp = time.time()
    except KeyboardInterrupt:
        print("[Manager] Interrupted.")
    finally:
        print(f"\n[Manager] Time's up. Simulating CRASH (kill -9)...")
        p.kill()
        p.join()
        
        print("\n--- Storage Stats ---")
        if os.path.exists(CHECKPOINT_FILE):
            print(f"  Checkpoint Size: {os.path.getsize(CHECKPOINT_FILE)/(1024*1024):.2f} MB")
            
        recover_system(mem_size)