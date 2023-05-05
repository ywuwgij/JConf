import socket
import os
import uuid

server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

if os.path.exists('/tmp/my_socket'):
    os.remove('/tmp/my_socket')

server_socket.bind('/tmp/my_socket')
server_socket.listen(1)

while True:
    connection, address = server_socket.accept()
    connection.sendall(uuid.uuid4().bytes)
    print("Data sent to client")
    connection.close()