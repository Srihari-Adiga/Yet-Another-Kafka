#Hardcode Zookeeper For Multiple Brokers
import socket
import threading

port = 65532

zoo = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
zoo.bind(('',port))
zoo.connect(('127.0.0.1', 11111))
message=zoo.recv(1024).decode('ascii')
if(message=="NICK:"):
    zoo.send('Z'.encode('ascii'))
f = open("ports.txt", "w")

brokers = [1, 2, 3]
leader = 1
f.write("11111")
f.close()
#print("how are you")

def evaluate():
    global zoo
    global leader
    while True:
        try:
            zoo.send('Z'.encode('ascii'))
            response_message = zoo.recv(1024).decode('ascii')
            
        except socket.error as e:
            print("Broker",leader," is down")
            last_ele = brokers.pop(0)
            brokers.append(last_ele)
            if(leader==1):
                
                zoo = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                zoo.connect(('127.0.0.1', 11112))
                leader = 2
                f = open("ports.txt", "w")
                f.write("11112")
                f.close()
                
            elif(leader==2):
                zoo = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                zoo.connect(('127.0.0.1', 11113))
                leader = 3
                f = open("ports.txt", "w")
                f.write("11113")
                f.close()

            else:
                zoo = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                zoo.connect(('127.0.0.1', 11111))
                leader = 1
                f = open("ports.txt", "w")
                f.write("11111")
                f.close()

evaluate_thread=threading.Thread(target=evaluate)
evaluate_thread.start()

f.close()