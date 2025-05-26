import socket
import os
import base64
import concurrent.futures
import threading
import time
import sys

HOST = '0.0.0.0'
PORT = 1234
Buffer_size = 1058576       
STORAGE_DIR = 'server_files'
TIMEOUT = 60                 

os.makedirs(STORAGE_DIR, exist_ok=True)

count_success = 0
count_fail = 0
counter_lock = threading.Lock()

def client_handler(conn, client_addr):
    global count_success, count_fail
    print(f"[+] Connected: {client_addr}")
    try:
        conn.settimeout(TIMEOUT)
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        raw = conn.recv(Buffer_size).decode()
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
            items = os.listdir(STORAGE_DIR)
            resp = '\n'.join(items) if items else 'No files available.'
            conn.send(resp.encode())
            with counter_lock:
                count_success += 1

        elif cmd == 'UPLOAD':
            if len(parts) < 2:
                conn.send(b'No filename provided')
                with counter_lock:
                    count_fail += 1
                return
            fname = parts[1]
            conn.send(b'READY')
            data_buf = b''
            while True:
                chunk = conn.recv(Buffer_size)
                if b'__END__' in chunk:
                    data_buf += chunk.replace(b'__END__', b'')
                    break
                data_buf += chunk

            try:
                content = base64.b64decode(data_buf)
                path = os.path.join(STORAGE_DIR, fname)
                with open(path, 'wb') as f:
                    f.write(content)
                conn.send(b'Upload OK')
                with counter_lock:
                    count_success += 1
            except Exception as e:
                conn.send(f'Upload error: {e}'.encode())
                with counter_lock:
                    count_fail += 1

        elif cmd == 'DOWNLOAD':
            if len(parts) < 2:
                conn.send(b'No filename provided')
                with counter_lock:
                    count_fail += 1
                return
            fname = parts[1]
            path = os.path.join(STORAGE_DIR, fname)
            if not os.path.isfile(path):
                conn.send(b'File not found')
                with counter_lock:
                    count_fail += 1
                return

            try:
                with open(path, 'rb') as f:
                    while True:
                        chunk = f.read(Buffer_size)
                        if not chunk:
                            break
                        conn.sendall(base64.b64encode(chunk))
                conn.send(b'__END__')
                with counter_lock:
                    count_success += 1
            except Exception as e:
                conn.send(f'Download error: {e}'.encode())
                with counter_lock:
                    count_fail += 1

        else:
            conn.send(b'Unknown command')
            with counter_lock:
                count_fail += 1

    except Exception as e:
        print(f"[!] Error handling {client_addr}: {e}")
        try:
            conn.send(f'Error: {e}'.encode())
        except:
            pass
        with counter_lock:
            count_fail += 1
    finally:
        conn.close()

def status_reporter():
    global count_success, count_fail
    while True:
        time.sleep(10)
        with counter_lock:
            print(f"[STATUS] Success: {count_success}, Failures: {count_fail}")

def run_server(worker_count=5):
    threading.Thread(target=status_reporter, daemon=True).start()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((HOST, PORT))
        sock.listen(100)
        print(f"Listening on {HOST}:{PORT} with {worker_count} threads")
        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as pool:
            while True:
                conn, addr = sock.accept()
                pool.submit(client_handler, conn, addr)

if __name__ == '__main__':
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    run_server(workers)
