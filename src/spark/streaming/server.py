import socket
import time


def run_server(port=9999):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    server_socket.bind(("localhost", port))

    server_socket.listen(1)
    print(f"Server listening on port {port}...")

    client_socket, addr = server_socket.accept()
    print(f"Received connection from {addr}")

    try:
        count = 0

        while True:
            message = f"Message number {count}\n"
            client_socket.send(message.encode("utf-8"))

            count += 1
            time.sleep(1)

    finally:
        client_socket.close()
        server_socket.close()


if __name__ == "__main__":
    run_server()
