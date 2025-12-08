import time
import multiprocessing
import random
import os
import signal
import uuid

# --- Configuration ---
MEMORY_SIZE_MB = 128
TOTAL_RUN_TIME_SEC = 25
COMMIT_CHANCE = 0.90

# --- Files ---
WAL_FILE = 'wal.log'

# --- Global Resources ---
memory_state = None

# --- Worker Logic ---
def worker_process(mem_size_bytes, return_dict):
    global memory_state
    
    memory_state = bytearray(mem_size_bytes)
    print(f"[Worker PID: {os.getpid()}] Starting Model 1 (WAL-Only)...")

    transaction_count = 0
    last_report_time = time.time()
    
    # [New] Variables for global average throughput calculation
    total_tx_lifetime = 0
    start_time_global = time.time()

    try:
        with open(WAL_FILE, 'ab', buffering=0) as wal_file:
            print(f"[Worker] WAL file opened. Processing transactions...")
            
            while True:
                # 1. Simulate Transaction (Insert/Update)
                tx_id = str(uuid.uuid4())[:8]
                num_ops = random.randint(1, 5)
                undo_log = []
                
                # Execution Phase: Write to Log + Dirty Memory
                for _ in range(num_ops):
                    address = random.randint(0, len(memory_state) - 1)
                    new_val = random.randint(0, 255)
                    old_val = memory_state[address]
                    
                    undo_log.append((address, old_val))
                    
                    # Log Entry: Redo and Undo info
                    entry = f"UPDATE:{tx_id}:{address}:{old_val}:{new_val}\n".encode('utf-8')
                    wal_file.write(entry)
                    
                    # Dirty the Page (Memory)
                    memory_state[address] = new_val
                
                # 2. Commit or Rollback
                if random.random() < COMMIT_CHANCE:
                    # COMMIT PATH
                    wal_file.write(f"COMMIT:{tx_id}\n".encode('utf-8'))
                    # CRITICAL: fsync forces durability. This is the bottleneck.
                    os.fsync(wal_file.fileno())
                    
                    transaction_count += 1
                    total_tx_lifetime += 1  # [New] Accumulate total successful transactions
                else:
                    # ROLLBACK PATH
                    # Active Undo in Memory
                    for address, old_val in reversed(undo_log):
                        memory_state[address] = old_val
                    
                    wal_file.write(f"ROLLBACK:{tx_id}\n".encode('utf-8'))
                    os.fsync(wal_file.fileno())
                
                # 3. Throughput Reporting
                current_time = time.time()
                if current_time - last_report_time >= 1.0:
                    # Instantaneous TPS
                    tps = transaction_count / (current_time - last_report_time)
                    print(f"[Worker] Throughput: {tps:.2f} TPS")
                    
                    # [New] Calculate and update global average TPS (save to shared dict)
                    total_duration = current_time - start_time_global
                    if total_duration > 0:
                        avg_tps = total_tx_lifetime / total_duration
                        return_dict['avg_tps'] = avg_tps
                    
                    transaction_count = 0
                    last_report_time = current_time

    except KeyboardInterrupt:
        pass
    finally:
        print(f"[Worker] Exiting.")

# --- Recovery System (Full Replay) ---
def recover_system(mem_size_bytes):
    print("\n[Recovery] Starting Recovery (Model 1: WAL-Only)...")
    total_start = time.time()
    
    # 1. Start from Empty (No Checkpoint)
    print("[Recovery] Phase 1: Initializing empty memory (No Checkpoint found).")
    recovered_memory = bytearray(mem_size_bytes)
    
    # 2. Analysis Phase (Scan entire log)
    print(f"[Recovery] Phase 2: Analyzing entire WAL...")
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
    print(f"[Recovery] Analysis complete. Inflight transactions: {len(inflight)}")

    # 3. Redo Phase (Replay Everything)
    print("[Recovery] Phase 3: Redo (Replaying History)...")
    redo_start = time.time()
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
    redo_end = time.time()

    # 4. Undo Phase (Revert Inflight)
    print("[Recovery] Phase 4: Undo (Reverting Inflight Data)...")
    undo_start = time.time()
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
    undo_end = time.time()

    total_end = time.time()
    print(f"\n[Recovery] Statistics:")
    print(f"  - Redo Duration: {redo_end - redo_start:.4f}s")
    print(f"  - Undo Duration: {undo_end - undo_start:.4f}s")
    print(f"  - Total Recovery Time: {total_end - total_start:.4f}s")

# --- Manager ---
if __name__ == "__main__":
    if os.path.exists(WAL_FILE): os.remove(WAL_FILE)
            
    print(f"[Manager] Experiment: Model 1 (WAL-Only)")
    mem_size = MEMORY_SIZE_MB * 1024 * 1024
    
    # [New] Use Manager to receive data from child process
    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    
    # Pass return_dict to worker
    p = multiprocessing.Process(target=worker_process, args=(mem_size, return_dict))
    p.start()
    
    try:
        time.sleep(TOTAL_RUN_TIME_SEC)
    except KeyboardInterrupt:
        print("[Manager] Interrupted.")
    finally:
        print(f"\n[Manager] Time's up. Simulating CRASH (kill -9)...")
        p.kill()
        p.join()
        
        print("\n--- Final Statistics ---")
        # [New] Output Average TPS at the end
        print(f"  Average Throughput: {return_dict.get('avg_tps', 0):.2f} TPS")
        
        print("\n--- Storage Stats ---")
        if os.path.exists(WAL_FILE):
            print(f"  WAL Size: {os.path.getsize(WAL_FILE)/1024:.2f} KB")
        
        recover_system(mem_size)
