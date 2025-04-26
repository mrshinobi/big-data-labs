import socket
import time
import datetime


def run_server(file_path, port=9999, delay=1):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    server_socket.bind(("localhost", port))

    server_socket.listen(1)
    print(f"Server listening on port {port}...")

    client_socket, addr = server_socket.accept()
    print(f"Received connection from {addr}")

    try:
        with open(file_path, "r", encoding="utf-8") as file:
            line_count = 0
            for line in file:
                line_count += 1
                if not line.strip():
                    continue

                timestamp = datetime.datetime.now(datetime.UTC)
                # message = f"{timestamp} {line}\n"
                message = line

                client_socket.send(message.encode("utf-8"))
                time.sleep(delay)  # wait a bit between lines
                print(f"Sent: {line_count} {timestamp} {message.strip()}")
    finally:
        client_socket.close()
        server_socket.close()


if __name__ == "__main__":
    project_dir = "/Users/katana/projects/uwm/big-data-labs"
    books_dir = f"{project_dir}/data/books/"

    run_server(file_path=f"{books_dir}/science.txt")
