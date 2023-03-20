import socket
import threading



f = open("ports.txt", "r")
port = f.read()
print(port)
#port = port[0:5]
port = int(port)
client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
import time
client.connect(('127.0.0.1',port))


pub_sub = input("Enter 'P' to join as publisher and 'S' to join as subscribers: ")
if(pub_sub=='S' or pub_sub=='s'):
    flag=int(input("Enter 1 to Read all the messages published and 0 to read the latest message: "))

def receive():
    global client
    while True:
        try:
            message=client.recv(1024).decode('ascii')
            if(message=="NICK:"):
                client.send(pub_sub.encode('ascii'))

            else:
                print(message)
        except:
            time.sleep(15)
            fp = open('ports.txt','r')
            data = fp.read()
            fp.close()
            leader_port = int(data)
            print(leader_port)
            client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            client.connect(('127.0.0.1',leader_port))
            message=client.recv(1024).decode('ascii')
            if(message=="NICK:"):
                client.send(pub_sub.encode('ascii'))

            else:
                print(message)

def write():
    while True:
        if(pub_sub=='P' or pub_sub=='p'):
            topic_name = input("Enter the topic name: ")
            message = input("Enter the message you want to publish: ")
            final_str = pub_sub+","+topic_name+","+message
            client.send(final_str.encode('ascii'))
        elif(pub_sub=='S' or pub_sub=='s'):
            topic_name = input("Enter the topic name: ")
            final_str = pub_sub+","+topic_name+","+str(flag)
            client.send(final_str.encode('ascii'))

recieve_thread=threading.Thread(target=receive)
recieve_thread.start()
time.sleep(1)
write_thread=threading.Thread(target=write)
write_thread.start()