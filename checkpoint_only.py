import time
import multiprocessing
import random
import os
import signal

# --- Configuration ---
MEMORY_SIZE_MB = 128
TOTAL_RUN_TIME_SEC = 25
CHECKPOINT_FREQUENCY_SEC = 5

# --- Files ---
CHECKPOINT_FILE = 'checkpoint.dat'
CHECKPOINT_TEMP = 'checkpoint.dat.tmp'

# --- Global Resources ---
memory_state = None
checkpoint_requested = False

# Global statistics variables
checkpoint_count = 0
total_bytes_written = 0

# --- Checkpoint Logic ---
def worker_checkpoint_handler(signum, frame):
    global checkpoint_requested
    checkpoint_requested = True

def perform_checkpoint():
    global checkpoint_requested, memory_state, checkpoint_count, total_bytes_written
    
    # print(f"[Worker] === Starting Checkpoint #{checkpoint_count + 1} ===")
    start = time.time()
    
    current_size = 0
    try:
        # Full flush to disk
        with open(CHECKPOINT_TEMP, 'wb') as f:
            f.write(memory_state)
            f.flush()
            os.fsync(f.fileno())
        
        current_size = os.path.getsize(CHECKPOINT_TEMP)
        os.rename(CHECKPOINT_TEMP, CHECKPOINT_FILE)
        
        checkpoint_count += 1
        total_bytes_written += current_size
        
    except Exception as e:
        print(f"Checkpoint Error: {e}")
        return
    
    end = time.time()
    # Log size explicitly
    print(f"[Worker] Checkpoint #{checkpoint_count} Finished | "
          f"Size: {current_size/(1024*1024):.2f} MB | "
          f"Total Written: {total_bytes_written/(1024*1024):.2f} MB | "
          f"Time: {end - start:.4f}s")
    checkpoint_requested = False

# --- Worker Logic ---
def worker_process(mem_size_bytes, return_dict):
    global memory_state, checkpoint_requested
    
    signal.signal(signal.SIGUSR1, worker_checkpoint_handler)
    memory_state = bytearray(mem_size_bytes)
    print(f"[Worker PID: {os.getpid()}] Starting Model 2 (Checkpoint-Only)...")

    transaction_count = 0
    last_report_time = time.time()
    
    total_tx_lifetime = 0
    start_time_global = time.time()

    try:
        while True:
            if checkpoint_requested:
                perform_checkpoint()
            
            # Simulate Transaction
            num_ops = random.randint(1, 5)
            for _ in range(num_ops):
                address = random.randint(0, len(memory_state) - 1)
                val = random.randint(0, 255)
                memory_state[address] = val
            
            transaction_count += 1
            
            current_time = time.time()
            if current_time - last_report_time >= 1.0:
                duration = current_time - last_report_time
                tps = transaction_count / duration
                
                total_tx_lifetime += transaction_count
                total_duration = current_time - start_time_global
                avg_tps = total_tx_lifetime / total_duration if total_duration > 0 else 0
                
                # [New] Update shared dictionary so Manager can see it even if crashed
                return_dict['avg_tps'] = avg_tps
                
                print(f"[Worker] Throughput: {tps:.2f} TPS | Avg: {avg_tps:.2f} TPS")
                
                transaction_count = 0
                last_report_time = current_time

    except KeyboardInterrupt:
        pass
    finally:
        # Pass final stats back to Manager
        return_dict['total_written'] = total_bytes_written
        return_dict['checkpoint_count'] = checkpoint_count
        print(f"[Worker] Exiting.")

# --- Recovery System ---
def recover_system(mem_size_bytes):
    print("\n[Recovery] Starting Recovery (Model 2: Checkpoint-Only)...")
    total_start = time.time()
    
    cp_load_start = time.time()
    try:
        with open(CHECKPOINT_FILE, 'rb') as f:
            recovered_memory = bytearray(f.read())
        print(f"[Recovery] Checkpoint loaded ({len(recovered_memory)/(1024*1024):.2f} MB)")
    except FileNotFoundError:
        print("[Recovery] No Checkpoint found.")
        recovered_memory = bytearray(mem_size_bytes)
    cp_load_end = time.time()

    print(f"[Recovery] No WAL file exists. Transactions since last checkpoint are LOST.")
    
    print(f"\n[Recovery] Statistics:")
    print(f"  - Checkpoint Load Time: {cp_load_end - cp_load_start:.4f}s")
    print(f"  - Total Recovery Time: {time.time() - total_start:.4f}s")

# --- Manager ---
if __name__ == "__main__":
    for f in [CHECKPOINT_FILE, CHECKPOINT_TEMP]:
        if os.path.exists(f): os.remove(f)

    print(f"[Manager] Experiment: Model 2 (Checkpoint-Only)")
    mem_size = MEMORY_SIZE_MB * 1024 * 1024
    
    # Use Manager dict to get stats from child process
    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    
    p = multiprocessing.Process(target=worker_process, args=(mem_size, return_dict))
    p.start()
    
    start_time = time.time()
    last_cp = start_time
    
    try:
        while time.time() - start_time < TOTAL_RUN_TIME_SEC:
            time.sleep(1)
            if time.time() - last_cp >= CHECKPOINT_FREQUENCY_SEC:
                os.kill(p.pid, signal.SIGUSR1)
                last_cp = time.time()
    except KeyboardInterrupt:
        print("[Manager] Interrupted.")
    finally:
        print(f"\n[Manager] Time's up. Simulating CRASH (kill -9)...")
        p.kill()
        p.join()
        
        # Retrieve stats
        total_written = return_dict.get('total_written', 0)
        count = return_dict.get('checkpoint_count', 0)
        avg_tps = return_dict.get('avg_tps', 0)

        print("\n--- Final Statistics ---")
        print(f"  Average Throughput:          {avg_tps:.2f} TPS")
        
        print("\n--- Final Storage Stats ---")
        print(f"  Total Checkpoints Performed: {count}")
        print(f"  Single Checkpoint Size:      {mem_size / (1024*1024):.2f} MB (Fixed Full Dump)")
        print(f"  Total Data Written to Disk:  {total_written / (1024*1024):.2f} MB")
            
        recover_system(mem_size)
