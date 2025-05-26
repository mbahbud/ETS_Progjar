import socket
import os
import base64
import concurrent.futures
import threading
import sys
import time

HOST = '0.0.0.0'
PORT = 1234
buffer_size = 1058576
STORAGE_DIR = 'server_files'
TIMEOUT = 60

os.makedirs(STORAGE_DIR, exist_ok=True)

count_success = 0
count_fail = 0
counter_lock = threading.Lock()

def worker_process(cmd, args):
    try:
        if cmd == 'LIST':
            files = os.listdir(STORAGE_DIR)
            out = '\n'.join(files) if files else 'No files available.'
            return out.encode()

        elif cmd == 'UPLOAD':
            name, data = args
            content = base64.b64decode(data)
            path = os.path.join(STORAGE_DIR, name)
            with open(path, 'wb') as f:
                f.write(content)
            return b'Upload OK'

        elif cmd == 'DOWNLOAD':
            name = args
            path = os.path.join(STORAGE_DIR, name)
            if not os.path.isfile(path):
                return b'File not found'
            with open(path, 'rb') as f:
                raw = f.read()
            return base64.b64encode(raw) + b'__END__'

        else:
            return b'Unknown command'
    except Exception as e:
        return f'Error: {e}'.encode()

def status_reporter():
    global count_success, count_fail
    while True:
        time.sleep(10)
        with counter_lock:
            print(f"[STATUS] Success: {count_success}, Failures: {count_fail}")

def client_handler(conn, addr, pool):
    global count_success, count_fail
    print(f"[+] Connection from {addr}")
    try:
        conn.settimeout(TIMEOUT)
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        raw = conn.recv(buffer_size).decode()
        if not raw:
            with counter_lock:
                count_fail += 1
            return

        parts = raw.strip().split()
        if not parts:
            conn.send(b'Invalid command')
            with counter_lock:
                count_fail += 1
            return

        cmd = parts[0].upper()

        if cmd == 'LIST':
            fut = pool.submit(worker_process, 'LIST', None)
            conn.sendall(fut.result())
            with counter_lock:
                count_success += 1

        elif cmd == 'UPLOAD':
            if len(parts) < 2:
                conn.send(b'No filename')
                with counter_lock:
                    count_fail += 1
                return
            name = parts[1]
            conn.send(b'READY')
            buf = b''
            while True:
                chunk = conn.recv(buffer_size)
                if b'__END__' in chunk:
                    buf += chunk.replace(b'__END__', b'')
                    break
                buf += chunk
            fut = pool.submit(worker_process, 'UPLOAD', (name, buf))
            res = fut.result()
            conn.sendall(res)
            with counter_lock:
                count_success += b'success' in res.lower()

        elif cmd == 'DOWNLOAD':
            if len(parts) < 2:
                conn.send(b'No filename')
                with counter_lock:
                    count_fail += 1
                return
            name = parts[1]
            fut = pool.submit(worker_process, 'DOWNLOAD', name)
            data = fut.result()
            if b'file not found' in data.lower():
                conn.sendall(data)
                with counter_lock:
                    count_fail += 1
                return
            for i in range(0, len(data), buffer_size):
                conn.sendall(data[i:i+buffer_size])
            with counter_lock:
                count_success += 1

        else:
            conn.send(b'Unknown command')
            with counter_lock:
                count_fail += 1

    except Exception as e:
        print(f"[!] Error with {addr}: {e}")
        try:
            conn.send(f'Error: {e}'.encode())
        except:
            pass
        with counter_lock:
            count_fail += 1
    finally:
        conn.close()

def run_server(workers=5):
    threading.Thread(target=status_reporter, daemon=True).start()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.bind((HOST, PORT))
        srv.listen(100)
        print(f"Listening on {HOST}:{PORT} with {workers} processes")
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
            while True:
                conn, addr = srv.accept()
                threading.Thread(target=client_handler, args=(conn, addr, pool)).start()

if __name__ == '__main__':
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    run_server(n)
