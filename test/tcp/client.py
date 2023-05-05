import socket
import os

client_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

if os.path.exists('/tmp/my_socket'):
    client_socket.connect('/tmp/my_socket')
else:
    print("Server is not available")

while True:
    data = client_socket.recv(1024)
    if not data:
        break
    print("Data received from server")
client_socket.close()