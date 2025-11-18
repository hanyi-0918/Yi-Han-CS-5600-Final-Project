import time
import multiprocessing
import random
import os
import signal
import uuid
import threading

MEMORY_SIZE_MB = 128
TOTAL_RUN_TIME_SEC = 25
CHECKPOINT_FREQUENCY_SEC = 10
COMMIT_CHANCE = 0.90
NUM_WORKER_THREADS = 4

WAL_FILE = 'wal.log'
CHECKPOINT_FILE = 'checkpoint.dat'
CHECKPOINT_TEMP = 'checkpoint.dat.tmp'

memory_state = None
checkpoint_requested = False
master_lock = None
wal_file_handle = None

def worker_checkpoint_handler(signum, frame):
    global checkpoint_requested
    print(f"\n[Process {os.getpid()}] Signal received: Checkpoint requested.\n")
    checkpoint_requested = True

def perform_checkpoint_locked():
    global memory_state, wal_file_handle
    print(f"[Thread {threading.current_thread().name}] === Starting Checkpoint (Holding Global Lock) ===")
    start_time = time.time()
    try:
        with open(CHECKPOINT_TEMP, 'wb') as f:
            f.write(memory_state)
            f.flush()
            os.fsync(f.fileno())
        os.rename(CHECKPOINT_TEMP, CHECKPOINT_FILE)
    except Exception as e:
        print(f"Checkpoint Failed: {e}")
        return
    wal_file_handle.close()
    with open(WAL_FILE, 'wb') as f:
        f.truncate(0)
    wal_file_handle = open(WAL_FILE, 'ab', buffering=0)
    end_time = time.time()
    print(f"[Thread {threading.current_thread().name}] === Checkpoint Finished. Pause Duration: {end_time - start_time:.4f}s ===\n")

def worker_thread_logic():
    global memory_state, checkpoint_requested, master_lock, wal_file_handle
    tx_count = 0
    last_report_time = time.time()
    while True:
        master_lock.acquire()
        try:
            if checkpoint_requested:
                perform_checkpoint_locked()
                checkpoint_requested = False
            tx_id = str(uuid.uuid4())[:8]
            num_ops = random.randint(1, 5)
            undo_log = []
            for _ in range(num_ops):
                address = random.randint(0, len(memory_state) - 1)
                new_data = random.randint(0, 255)
                old_data = memory_state[address]
                undo_log.append((address, old_data))
                entry = f"UPDATE:{tx_id}:{address}:{old_data}:{new_data}\n".encode('utf-8')
                wal_file_handle.write(entry)
                memory_state[address] = new_data
            if random.random() < COMMIT_CHANCE:
                wal_file_handle.write(f"COMMIT:{tx_id}\n".encode('utf-8'))
                wal_file_handle.fsync()
                tx_count += 1
            else:
                for address, old_data in reversed(undo_log):
                    memory_state[address] = old_data
                wal_file_handle.write(f"ROLLBACK:{tx_id}\n".encode('utf-8'))
                wal_file_handle.fsync()
        finally:
            master_lock.release()
        current_time = time.time()
        if current_time - last_report_time >= 1.0:
            tps = tx_count / (current_time - last_report_time)
            print(f"[{threading.current_thread().name}] Throughput: {tps:.2f} TPS")
            tx_count = 0
            last_report_time = current_time

def worker_process_entry(mem_size_bytes):
    global memory_state, master_lock, wal_file_handle
    signal.signal(signal.SIGUSR1, worker_checkpoint_handler)
    memory_state = bytearray(mem_size_bytes)
    master_lock = threading.Lock()
    wal_file_handle = open(WAL_FILE, 'ab', buffering=0)
    print(f"[Process] Starting {NUM_WORKER_THREADS} threads with a GLOBAL LOCK strategy...")
    threads = []
    for i in range(NUM_WORKER_THREADS):
        t = threading.Thread(target=worker_thread_logic, name=f"Thread-{i+1}")
        t.daemon = True
        t.start()
        threads.append(t)
    while True:
        time.sleep(1)

def recover_system(mem_size_bytes):
    print("\n[Recovery] +++ System Recovery Started (ARIES-lite) +++")
    total_start = time.time()
    try:
        with open(CHECKPOINT_FILE, 'rb') as f:
            recovered_memory = bytearray(f.read())
        print(f"[Recovery] Phase 1: Checkpoint loaded ({len(recovered_memory)/(1024*1024):.2f} MB)")
    except FileNotFoundError:
        print("[Recovery] Phase 1: No Checkpoint found. Starting from empty state.")
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
                        if parts[1] not in tx_status:
                            tx_status[parts[1]] = "inflight"
                    elif parts[0] == "COMMIT":
                        tx_status[parts[1]] = "committed"
                    elif parts[0] == "ROLLBACK":
                        tx_status[parts[1]] = "aborted"
                except:
                    pass
    inflight = {tx for tx, status in tx_status.items() if status == "inflight"}
    print(f"[Recovery] Analysis complete. Found {len(inflight)} inflight transactions.")
    print("[Recovery] Phase 3: Redo (Replaying History)...")
    redo_count = 0
    for line_bytes in wal_lines:
        try:
            parts = line_bytes.decode('utf-8').strip().split(':')
            if parts[0] == "UPDATE":
                addr, new_val = int(parts[2]), int(parts[4])
                recovered_memory[addr] = new_val
                redo_count += 1
        except:
            pass
    print("[Recovery] Phase 4: Undo (Reverting Inflight Data)...")
    undo_count = 0
    for line_bytes in reversed(wal_lines):
        if not inflight:
            break
        try:
            parts = line_bytes.decode('utf-8').strip().split(':')
            if parts[0] == "UPDATE":
                tx_id = parts[1]
                if tx_id in inflight:
                    addr, old_val = int(parts[2]), int(parts[3])
                    recovered_memory[addr] = old_val
                    undo_count += 1
        except:
            pass
    total_end = time.time()
    print(f"\n[Recovery] Recovery Complete.")
    print(f"  - Redo Operations: {redo_count}")
    print(f"  - Undo Operations: {undo_count}")
    print(f"  - Total Time:      {total_end - total_start:.4f} sec")
    print("[Recovery] System is now consistent.\n")

if __name__ == "__main__":
    for f in [WAL_FILE, CHECKPOINT_FILE, CHECKPOINT_TEMP]:
        if os.path.exists(f):
            os.remove(f)
    print(f"[Manager] Experiment: Model 3.1 (Hybrid + {NUM_WORKER_THREADS} Threads + Global Lock)")
    mem_size = MEMORY_SIZE_MB * 1024 * 1024
    p = multiprocessing.Process(target=worker_process_entry, args=(mem_size,))
    p.start()
    start_time = time.time()
    last_checkpoint = start_time
    try:
        while time.time() - start_time < TOTAL_RUN_TIME_SEC:
            time.sleep(1)
            if time.time() - last_checkpoint >= CHECKPOINT_FREQUENCY_SEC:
                print("\n[Manager] Triggering Checkpoint Signal...")
                os.kill(p.pid, signal.SIGUSR1)
                last_checkpoint = time.time()
    except KeyboardInterrupt:
        print("[Manager] Interrupted.")
    finally:
        print(f"\n[Manager] Time's up ({TOTAL_RUN_TIME_SEC}s). Simulating CRASH (kill -9)...")
        p.kill()
        p.join()
        print("[Manager] Process crashed.")
        print("\n--- Storage Stats ---")
        if os.path.exists(WAL_FILE):
            print(f"  WAL Size:        {os.path.getsize(WAL_FILE)/1024:.2f} KB")
        if os.path.exists(CHECKPOINT_FILE):
            print(f"  Checkpoint Size: {os.path.getsize(CHECKPOINT_FILE)/(1024*1024):.2f} MB")
        recover_system(mem_size)
