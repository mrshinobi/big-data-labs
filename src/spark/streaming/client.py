import socket


def run_client(server_host="localhost", server_port=9999):
    # create a socket object
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # connect to the server
    client_socket.connect((server_host, server_port))
    print(f"Connected to server at {server_host} on port {server_port}")

    try:
        while True:
            data = client_socket.recv(1024)  # bufor 1024b
            if not data:
                print("No more data from server.")
                break
            print("Received:", data.decode("utf-8"))
    finally:
        client_socket.close()


if __name__ == "__main__":
    run_client()
