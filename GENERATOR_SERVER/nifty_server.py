import socket
import time
import random
import json
from datetime import datetime




def generate_random_data():
    data  = {"date":str(datetime.now().date()),
            "timestamp": str(datetime.now().time()), 
            "symbol":"NIFTY50FUT25",
            "open":None,
            "high":None,
            "low":None,
            "close":None,
            }
    open_min,open_max = 23445.0, 23600
    open_price = round(random.uniform(open_min,open_max), 2)
    data['open'] = open_price
    # Generate a random High price (must be greater than Open)
    high_price = round(open_price + random.uniform(0.01, 10.0), 2)
    data['high'] =high_price
    # Generate a random Low price (must be less than Open)
    low_price = round(open_price - random.uniform(0.01, 10.0), 2)
    data['low'] =low_price
    # Generate a random Close price (must be between Low and High)
    close_price = round(random.uniform(low_price, high_price), 2)
    data['close'] =close_price
    
    return data


#========================================================================================================
def start_server(host,port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(1)
    print(f"Server 2 listening on port {port}...")

    conn, addr = server_socket.accept()
    print(f"Connection from {addr} established.")

    while True:
        data = generate_random_data()

        conn.sendall(json.dumps(data).encode("utf-8"))
        time.sleep(1)  
    conn.close()
    server_socket.close()

if __name__ == "__main__":
    #----------------------------|
    host  = ''         #| place your host IP
    port  = 0          #| place your host port
    #----------------------------|
    start_server(host,port)
















