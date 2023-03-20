import threading
from socket import*
import time
import os
import json  
import joblib
path="D://ASSIGNMENTS//SEM-5//BD//Project//Project//"

host='127.0.0.1'
port= 11113

server_socket=socket(AF_INET,SOCK_STREAM)
server_socket.bind(('',port))
server_socket.listen()

while(1):
    time.sleep(2)
    fp=open(path+"ports.txt")
    d=int(fp.read())
    fp.close()
    if d==11113:
        break




publishers = []
subscribers = []

fp=open(path+"topics_publishers.txt","r")
data=fp.read()
fp.close()
topics_publishers = json.loads(data) # stored
print(topics_publishers)
fp=open(path+"topics_subscribers.txt","r")
data=fp.read()
fp.close()
topics_subscribers = json.loads(data) # stored
print(topics_subscribers)
fp=open(path+"topic_partition.txt","r")
data=fp.read()
fp.close()
topic_partition=json.loads(data) # stored
fp=open(path+"topics_subscribers_flag0.txt","r")
data=fp.read()
fp.close()
topics_subscribers_flag0 = json.loads(data) # stored
print(topics_subscribers_flag0)
zookeeper = []

def broadcast(topic_name,frombegining):
    PATH="D://ASSIGNMENTS//SEM-5//BD//Project//Project//"+topic_name
    fp=open(PATH+"//"+"Partition0.txt")
    list_p0=fp.readlines()
    fp.close()
    fp=open(PATH+"//"+"Partition1.txt")
    list_p1=fp.readlines()
    fp.close()
    fp=open(PATH+"//"+"Partition2.txt")
    list_p2=fp.readlines()
    fp.close()
    print(frombegining)
    print(type(frombegining))
    if frombegining==1:
        i=0
        print("in if")
        string=""
        l=[len(list_p1),len(list_p2),len(list_p0)]
        print(l)
        maxlen=max(l)
        print("max",maxlen)
        for i in range(maxlen):
            if i < len(list_p0):
                string+=list_p0[i]
            if i < len(list_p1):
                string+=list_p1[i]
            if i < len(list_p2):
                string+=list_p2[i] 
        for subscriber in subscribers:
            print("*")
            if(str(subscriber) in topics_subscribers[topic_name] and str(subscriber) not in topics_subscribers_flag0[topic_name] ):
                subscriber.send(string.encode("ascii"))        
                
  
    

def handle_client(client):

    while True:
        try:
            message=client.recv(2048).decode('ascii')
            messages = message.split(',')
            print(messages,"IN handle client")
            directories=os.listdir("D://ASSIGNMENTS//SEM-5//BD//Project//Project")
            if(messages[0]=='Z'):
                client.send("I am alive!".encode('ascii'))
            if(messages[0]=='P' or messages[0]=='p'):
                topic_name = messages[1]
                content = messages[2]
                PATH="D://ASSIGNMENTS//SEM-5//BD//Project//Project//"+topic_name
                if topic_name not in directories:
                    os.mkdir(PATH)
                    fp = open(PATH+"//"+"Partition0.txt", 'x')
                    fp.close()
                    fp = open(PATH+"//"+"Partition1.txt", 'x')
                    fp.close()
                    fp = open(PATH+"//"+"Partition2.txt", 'x')
                    fp.close()

                if(topic_name not in topics_publishers and topic_name not in topic_partition):
                    topic_partition[topic_name]=0
                    topics_publishers[topic_name]=[]
                    topics_publishers[topic_name].append(str(client))
                    p="Partition"+str(topic_partition[topic_name])+".txt"
                    topic_partition[topic_name]=(topic_partition[topic_name]+1)%3
                    fp=open(path+"//"+"topic_partition.txt",'w')
                    fp.write(json.dumps(topic_partition))
                    fp.close()
                    fp = open(PATH +"//"+ p, 'a')
                    fp.write(content+'\n')
                    fp.close()
                    # joblib.dump(topics_publishers, "D://ASSIGNMENTS//SEM-5//BD//Project//Project//topics_publishers.sav")
                    fp=open(path+"//"+"topics_publishers.txt",'w')
                    fp.write(json.dumps(topics_publishers))
                    fp.close()
                else:
                    if str(client) not in topics_publishers[topic_name]:
                        topics_publishers[topic_name].append(str(client))
                    p="Partition"+str(topic_partition[topic_name])+".txt"
                    topic_partition[topic_name]=(topic_partition[topic_name]+1)%3
                    fp=open(path+"//"+"topic_partition.txt",'w')
                    fp.write(json.dumps(topic_partition))
                    fp.close()
                    fp = open(PATH +"//"+ p, 'a')
                    fp.write(content+'\n')
                    fp.close()

             
                for subscriber in subscribers:
                    print(subscriber,topics_subscribers_flag0[topic_name])
                    if(str(subscriber) in topics_subscribers_flag0[topic_name]):
                        print("Flag0 client")
                        subscriber.send(content.encode("ascii"))

                broadcast(topic_name,1)        
                
            elif(messages[0]=='S' or messages[0]=='s'):
                topic_name = messages[1]
                
                flag=int(messages[2])
                
                print(flag,type(flag),topic_name)
                if(flag==0):
                    if(topic_name not in topics_subscribers_flag0):
                        topics_subscribers_flag0[topic_name]=[]
                        topics_subscribers_flag0[topic_name].append(str(client))
                        print("added")
                    else:
                        if(str(client) not in topics_subscribers_flag0[topic_name]):
                            topics_subscribers_flag0[topic_name].append(str(client))
                            print("added")
                    # joblib.dump(topics_subscribers_flag0, "D://ASSIGNMENTS//SEM-5//BD//Project//Project//topics_subscribers_flag0.sav")        
                    
                    fp=open(path+"//"+"topics_subscribers_flag0.txt",'w')
                    fp.write(json.dumps(topics_subscribers_flag0))
                    fp.close()
                    
                if(topic_name not in directories):
                    client.send("No such topic available!!".encode('ascii'))
                else:
                    if(topic_name not in topics_subscribers):
                        topics_subscribers[topic_name]=[]
                        topics_subscribers[topic_name].append(str(client))
                        print("added")
                    else:
                        if(str(client) not in topics_subscribers[topic_name]):
                            topics_subscribers[topic_name].append(str(client))
                            print("added")
                    # joblib.dump(topics_subscribers, "D://ASSIGNMENTS//SEM-5//BD//Project//Project//topics_subscribers.sav")        
                    fp=open(path+"//"+"topics_subscribers.txt",'w')
                    fp.write(json.dumps(topics_subscribers))
                    fp.close()        
                    broadcast(topic_name,int(messages[2]))        
                    
                    
        except Exception as e:
            for x in topics_publishers.keys():
                if(str(client) in topics_publishers[x]):
                    topics_publishers[x].remove(str(client))
            fp=open(path+"//"+"topics_publishers.txt",'w')
            fp.write(json.dumps(topics_publishers))
            fp.close()
            for x in topics_subscribers.keys():
                if(str(client) in topics_subscribers[x]):
                    topics_subscribers[x].remove(str(client))
            fp=open(path+"//"+"topics_subscribers.txt",'w')
            fp.write(json.dumps(topics_subscribers))
            fp.close() 
            for x in topics_subscribers_flag0.keys():
                if(str(client) in topics_subscribers_flag0[x]):
                    topics_subscribers_flag0[x].remove(str(client))
            fp=open(path+"//"+"topics_subscribers_flag0.txt",'w')
            fp.write(json.dumps(topics_subscribers_flag0))
            fp.close()
            if(client in subscribers):
                subscribers.remove(client)
            if(client in publishers):
                publishers.remove(client)
            
def receive():
    while True:

        client,client_addr=server_socket.accept()
        print("Connected with address:",client_addr,client)
        client.send("NICK:".encode('ascii'))
        client_identity=client.recv(2048).decode('ascii')
        if(client_identity=='Z' or client_identity=='z'):
            zookeeper.append(client)
        if(client_identity=='P' or client_identity=='p'):
            publishers.append(client)
        elif(client_identity=='S' or client_identity=='s'):
            subscribers.append(client)
        client.send("Successfully Connected to the broker!!".encode('ascii'))
        print(client_addr, "has connected successfully!")
        
        t1=threading.Thread(target=handle_client,args=((client),))
        t1.start()

print("Broker 3 is active..!!")
receive()
