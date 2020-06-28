#!usr/bin/python
#Author : Daniel Bayens
import pymysql.cursors
import pymysql
import paho.mqtt.client as mqtt
import getpass
import datetime
from random import randint



# Connect to the database
connection = pymysql.connect(host='127.0.0.1',
                             user='root2',
                             password='innovate',
                             db='PinIt',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

while connection.open == False:
    connection
    

# The callback for when the client receives a CONNACK response from the server
def on_connect(client, userdata, flags, rc):
    print("Connection set...")
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("/PinIt/Inf/#")


# Cur is used to call on the connection
cur = connection.cursor()



# The callback for when a PUBLISH message is received from the server
def on_message(client, userdata, msg):
    # Variable definitions depending on given topic and payload (MQTT)
    payload = str(msg.payload)
    user = str.split(msg.topic, "/")[-1]
    presence = str.split(payload, "@")[-1]
    
    if "/PinIt/Inf/Reminder/" in msg.topic or "/PinIt/Inf/Agenda/" in msg.topic:
        title = str.split(payload, "@")[-2]
        message = str.split(payload, "@")[-1]
        

    # Functions that get timestamps:
    # Return current date in format: y-m-d h-m-s
    def currentDate():
        dateTime = datetime.datetime.now()
        return dateTime.strftime("%Y-%m-%d %H:%M:%S")
    # Return current date in format: y-m-d
    def currentDate2():
        dateTime = datetime.datetime.now()
        return dateTime.strftime("%Y-%m-%d")
    # Return number of days from current date calculated with given number of days
    def endTime():
        numberOfDays = str.split(payload, "@")[-3]
        dateTime = datetime.datetime.now()
        endDateTime = dateTime + datetime.timedelta(days=float(numberOfDays))
        return endDateTime.strftime("%Y-%m-%d")

    # Function removes all messages from the database when the DateTimeVisible of 
    # the message is expired
    def cleanPosts():
        date = currentDate2()
        sql = "DELETE FROM POST WHERE `DateTimeVisible` < '%s' OR `AgendaTime` < '%s' " % (date, date)
        cur.execute(sql)
        connection.commit()
    
    # Checks to see if something is posted in topic 'Reminder'
    # If so, it inserts a reminder in the database with the corresponding user
    # and publishes a reminder in topic ReminderMessagesTMP(MQTT)
    if "/PinIt/Inf/Reminder/" in msg.topic:
        sql = "SELECT `UserID`, FirstName, LastName FROM `USER` WHERE AndroidID = '%s'" % (user)
        cur.execute(sql)
        for row in cur:
            name = row["FirstName"] + " " + row["LastName"]
            UserID = row["UserID"]
        # Call def to set date and visibility time of message
        date = currentDate()
        endDate = endTime()
        sql = "INSERT INTO `POST` (UserID, PostType, Title, DateTime, DateTimeVisible, Message) VALUES ('%s', 'Reminder', '%s', '%s', '%s', '%s')" % (UserID, title, date, endDate, message)
        cur.execute(sql)
        connection.commit()

        # Publish temporarily reminder in ReminderMessagesTMP
        RPayload = name + "@" + title + "@" + message
        RTopic = "/PinIt/Inf/ReminderMessagesTMP/"
        client.publish(RTopic, payload= RPayload, qos=0, retain=False)
    
    # Returns all reminders from database, each reminder has a separate publish
    # Also removes the messages from the database when the DateTimeVisible
    # are expired
    if "/PinIt/Inf/ReminderRequest/" in msg.topic:
        date = currentDate()
        # Only selects messages from which the DateTimeVisible have NOT expired
        sql = "SELECT `Title`, `Message`, FirstName, LastName FROM `POST`, USER WHERE POST.UserID = USER.UserID AND PostType = 'Reminder' AND DateTimeVisible >= '%s'  ORDER BY DateTime DESC" % (date)
        cur.execute(sql)
        connection.commit()
        # For each message a publish will be made
        for row in cur:
            txt = row["FirstName"] + " " + row["LastName"] + "@" + row["Title"] + "@" + row["Message"]
            RPayload = txt
            RTopic = "/PinIt/Inf/ReminderMessages/" + user
            client.publish(RTopic, payload= RPayload, qos=0, retain=False)

        # If message has expired, remove from database
        cleanPosts()

    # Checks to see if something is posted in topic 'Agenda'
    # If so, it inserts an agenda message in the database with the corresponding user
    # and publishes an agenda message in topic AgendaMessagesTMP(MQTT)    
    if "/PinIt/Inf/Agenda/" in msg.topic:
        sql = "SELECT `UserID` FROM `USER` WHERE AndroidID = '%s'" % (user)
        cur.execute(sql)
        UserID = cur.fetchone()["UserID"]
        # Call def to set date and visibility time of message
        date = currentDate()
        agendaTime = str.split(payload, "@")[-3]
        sql = "INSERT INTO `POST` (UserID, PostType, Title, Message, AgendaTime) VALUES ('%s', 'Agenda', '%s', '%s', '%s')" % (UserID, title, message, agendaTime)
        cur.execute(sql)
        connection.commit()
        
        # Publish temporarily reminder in AgendaMessagesTMP        
        RPayload = agendaTime + "@" + title + "@" + message
        RTopic = "/PinIt/Inf/AgendaMessagesTMP/"
        client.publish(RTopic, payload= RPayload, qos=0, retain=False)
    
    # Returns all reminders from database, each reminder has a separate publish
    # Also removes the messages from the database when the DateTimeVisible
    # are expired
    if "/PinIt/Inf/AgendaRequest/" in msg.topic:
        user = str.split(msg.topic, "/")[-1]
        date = currentDate2()
        sql = "SELECT `Title`, `Message`, `AgendaTime` FROM `POST` WHERE PostType = 'Agenda' AND AgendaTime >= '%s' ORDER BY AgendaTime" % (date)
        cur.execute(sql)
        connection.commit()
        for row in cur:
            txt = str(row["AgendaTime"]) + "@" + row["Title"] + "@" + row["Message"]
            RPayload = txt
            RTopic = "/PinIt/Inf/AgendaMessages/" + user
            client.publish(RTopic, payload= RPayload, qos=0, retain=False)
        
        # If message has expired, remove from database
        cleanPosts()

    # Code to change presence state
    if "/PinIt/Inf/Presence/" in msg.topic:
        androidID = msg.payload 
        sql = "SELECT `Presence`, `FirstName`, `LastName` FROM `USER` WHERE AndroidID = '%s'" % (androidID)
        cur.execute(sql)
        connection.commit()
        for row in cur:
            person = row["FirstName"] + " " + row["LastName"]
            presence = row["Presence"]
        if presence == 0:
            sql = "UPDATE USER SET Presence = 1 WHERE AndroidID = '%s'" % (androidID)
            cur.execute(sql)
            connection.commit()
            #RPayload = "%s %s is aanwezig" % (firstname, lastname)
            #client.publish("/PinIt/Inf/PresenceState/", payload= RPayload, qos=0, retain=False)
            RPayload = person + "@1"
            RTopic = "/PinIt/Inf/PresenceStateTMP/"
            client.publish(RTopic, payload= RPayload, qos=0, retain=False)
        elif presence == 1:
            sql = "UPDATE USER SET Presence = 0 WHERE AndroidID = '%s'" % (androidID)
            cur.execute(sql)
            connection.commit()
            #RPayload = "%s %s is afwezig" % (firstname, lastname)
            #client.publish("/PinIt/Inf/PresenceState/", payload= RPayload, qos=0, retain=False)
            RPayload = person + "@0"
            RTopic = "/PinIt/Inf/PresenceStateTMP/"
            client.publish(RTopic, payload= RPayload, qos=0, retain=False)
        else:
            RPayload = "Error: Presence"
            client.publish("/PinIt/Inf/Error/", payload= RPayload, qos=0, retain=False)

    # Code for users to check the presence of teachers
    if "/PinIt/Inf/PresenceRequest/" in msg.topic:
        user = str.split(msg.topic, "/")[-1]
        sql = "SELECT `FirstName`, `LastName` FROM `USER` WHERE Presence = 1"
        cur.execute(sql)
        connection.commit()
        present = ""
        for row in cur:
            txt = row["FirstName"] + " "  + row["LastName"]
            RPayload = txt
            RTopic = "/PinIt/Inf/PresenceState/" + user
            client.publish(RTopic, payload= RPayload, qos=0, retain=False)
    
    # Code for users to check the presence of teachers
    if "/PinIt/Inf/PresenceRequestUser/" in msg.topic:
        user = str.split(msg.topic, "/")[-1]
        sql = "SELECT `Presence` FROM `USER` WHERE AndroidID = '%s'" % (androidID)
        cur.execute(sql)
        connection.commit()
        RPayload = str(cur.fetchone()["Presence"]);
        RTopic = "/PinIt/Inf/PresenceRequestUserResponse/" + user
        client.publish(RTopic, payload= RPayload, qos=0, retain=False)        
     
    def createID():
            tmpUserID = randint(100000,999999)
            sql = "SELECT UserID FROM USER WHERE UserID = '%d'" % (tmpUserID)
            cur.execute(sql)
            connection.commit()
            result = cur.fetchone()
            if result == tmpUserID:
                createID()
            else:
                userID = tmpUserID
                return userID
    
    if "/PinIt/Inf/NewAccount/" in msg.topic:
        firstname = str.split(payload, "@")[-2]
        lastname = str.split(payload, "@")[-1]

        userID = createID()

        sql = "SELECT * FROM `USER` WHERE UserID = '%d'" % (userID)
        cur.execute(sql)
        connection.commit()
        result = cur.fetchone()
        if result is None:
            accountType = str.split(payload, "@")[-3]
            firstname = str.split(payload, "@")[-2]
            lastname = str.split(payload, "@")[-1]
            sql = "INSERT INTO `USER` (UserID, UserTypeID, FirstName, LastName) VALUES ('%s', '%s', '%s', '%s')" % (userID, accountType, firstname, lastname)
            cur.execute(sql)
            connection.commit()
            RPayload = "%s" % (userID)
            client.publish("/PinIt/Inf/NewAccountResponse/", payload= RPayload, qos=0, retain=False)
        else:
            RPayload = "Account with ID: %s allready exists." % (userID)
            client.publish("/PinIt/Inf/NewAccountResponse/", payload= RPayload, qos=0, retain=False)
    
    #CHECK IF ANDROID EXISTS
    if "/PinIt/Inf/AccountActivation/" in msg.topic:
        userID = str.split(msg.payload, "@")[-2]
        sql = "SELECT Active FROM `USER` WHERE UserID = '%s'" % (userID)
        cur.execute(sql)
        connection.commit()
        result = cur.fetchone()["Active"]
        if result == 1:
            RPayload = "This account is allready active."
            client.publish("/PinIt/Inf/AccountActivationResponse/", payload= RPayload, qos=0, retain=False)
        else:
            androidID = str.split(msg.payload, "@")[-1]
            sql = "UPDATE USER SET Active = 1, AndroidID = '%s' WHERE UserID = '%s'" % (androidID, userID)
            cur.execute(sql)
            connection.commit()
            sql = "SELECT Active FROM `USER` WHERE UserID = '%s'" % (userID)
            cur.execute(sql)
            connection.commit()
            result = cur.fetchone()["Active"]
            if result == 1:             
                sql = "SELECT FirstName FROM `USER` WHERE AndroidID = '%s'" % (androidID)
                cur.execute(sql)
                connection.commit()
                firstname = cur.fetchone()["FirstName"]
                if firstname is None:
                    RPayload = "Something went wrong."
                    client.publish("/PinIt/Inf/NewAccountResponse/", payload= RPayload, qos=0, retain=False)
                else:
                    RPayload = "Welcome %s, your account has been activated on this device." % (firstname)
                    client.publish("/PinIt/Inf/NewAccountResponse/", payload= RPayload, qos=0, retain=False)
            else:
                RPayload = "Account activation failed." % (userID)
                client.publish("/PinIt/Inf/NewAccountResponse/", payload= RPayload, qos=0, retain=False)

    if "/PinIt/Inf/AccCheck/" in msg.topic:
        userID = msg.payload
        sql = "SELECT AndroidID FROM `USER` WHERE UserID = %s" % (userID)
        cur.execute(sql)
        connection.commit()
        for row in cur:
            result = row["AndroidID"]
        if result > 0:
            RPayload = userID + " " + result
            client.publish("/PinIt/Inf/AccCheckResponse/", payload= RPayload, qos=0, retain=False)
        else:
            RPayload = "User hasn't got a valid Phone ID."
            client.publish("/PinIt/Inf/AccCheckResponse/", payload= RPayload, qos=0, retain=False)


# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Subscribe to MQTT
                
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set("Python", password="innovate")
client.connect("localhost", 1883)  
client.loop_forever()
connection.close()
