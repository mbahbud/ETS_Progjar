import socket
import os
import base64
import concurrent.futures
import time
import sys
from tqdm import tqdm

SERVER_IP = '172.16.16.101'
SERVER_PORT = 1234
Buffer_Size = 1058576
Client_DIR = 'client_files'
SAVE_DIR = 'client_downloads'

os.makedirs(SAVE_DIR, exist_ok=True)

def send_upload(fname):
    try:
        path = os.path.join(Client_DIR, fname)
        size = os.path.getsize(path)
        with socket.socket() as s:
            s.connect((SERVER_IP, SERVER_PORT))
            s.send(f'UPLOAD {fname}'.encode())
            resp = s.recv(Buffer_Size)
            if resp != b'READY':
                return False, f"Server not ready: {resp.decode()}"

            with open(path, 'rb') as f, tqdm(total=size, unit='B', unit_scale=True, desc=f'Uploading {fname}', leave=False) as p:
                while chunk := f.read(Buffer_Size):
                    s.send(base64.b64encode(chunk))
                    p.update(len(chunk))
            s.send(b'__END__')
            return True, s.recv(Buffer_Size).decode()
    except Exception as e:
        return False, str(e)

def send_download(fname):
    try:
        with socket.socket() as s:
            s.connect((SERVER_IP, SERVER_PORT))
            s.send(f'DOWNLOAD {fname}'.encode())
            buf = b''
            with tqdm(unit='B', unit_scale=True, desc=f'Downloading {fname}', leave=False) as p:
                while True:
                    part = s.recv(Buffer_Size)
                    if b'__END__' in part:
                        buf += part.replace(b'__END__', b'')
                        p.update(len(part) - len(b'__END__'))
                        break
                    buf += part
                    p.update(len(part))
            data = base64.b64decode(buf)
            out_path = os.path.join(SAVE_DIR, fname)
            with open(out_path, 'wb') as f:
                f.write(data)
            return True, "Download completed"
    except Exception as e:
        return False, str(e)

def task_worker(op, fname):
    start = time.time()
    if op == 'upload':
        ok, msg = send_upload(fname)
    else:
        ok, msg = send_download(fname)
    elapsed = time.time() - start
    path = os.path.join(Client_DIR if op=='upload' else SAVE_DIR, fname)
    size = os.path.getsize(path) if ok and os.path.exists(path) else 0
    speed = size / elapsed if elapsed > 0 else 0
    return ok, elapsed, speed, msg

def main():
    if len(sys.argv) != 5:
        print("Arguments: python3 file_client_pool.py [upload/download] [10MB|50MB|100MB] [workers] [thread|process]")
        return

    op, vol, workers, mode = sys.argv[1].lower(), sys.argv[2], int(sys.argv[3]), sys.argv[4].lower()
    mapping = {"10MB": "10MB.txt", "50MB": "50MB.txt", "100MB": "100MB.txt"}
    fname = mapping.get(vol)
    if not fname:
        print("Unknown volume")
        return

    Executor = concurrent.futures.ThreadPoolExecutor if mode=='thread' else concurrent.futures.ProcessPoolExecutor
    print(f"Starting {op} {fname} with {workers} workers using {mode}")

    total_t, total_speed, success, failures = 0, 0, 0, 0
    with Executor(max_workers=workers) as execr:
        futures = [execr.submit(task_worker, op, fname) for _ in range(workers)]
        for fut in concurrent.futures.as_completed(futures):
            ok, t, sp, msg = fut.result()
            if ok:
                success += 1
                total_t += t
                total_speed += sp
            else:
                failures += 1
            print(f"[{'OK' if ok else 'FAIL'}] Time: {t:.2f}s | Throughput: {sp:.2f}B/s | {msg}")

    avg_t = total_t / success if success else 0
    avg_sp = total_speed / success if success else 0
    print("\n--- Report ---")
    print(f"Operation      : {op.upper()}")
    print(f"File           : {fname}")
    print(f"Workers        : {workers}")
    print(f"Mode           : {mode}")
    print(f"Avg Time       : {avg_t:.2f}s")
    print(f"Avg Throughput : {avg_sp:.2f} B/s")
    print(f"Success        : {success}")
    print(f"Failures       : {failures}")

if __name__ == '__main__':
    main()
