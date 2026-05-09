import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic = 'transactions'

users = []
for i in range(1,21):
    users.append("User" + str(i))
merchants = ["Retail", "Electronics", "Groceries", "Online_Subscription", "Travel", "Dining"]
locations = ["Sri Lanka", "India", "USA", "England", "Singapore", "Dubai","New Zealand","Malysia"]  


user_countries = {}
for user in users:
    user_countries[user] = random.choice(locations) 
    
    

print("FinTech Fraud Data Producer Starts")


try:
    while True:
        isfraud=False
        isCountryFraud=False
        
        num = random.random() 
        countrynum=random.random()
        
        if(num<0.1):
            isfraud=True
            
        if(countrynum<0.1):
            isCountryFraud=True
        
            
        user_id=random.choice(users)
        merchant = random.choice(merchants)
        time_stamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        location=user_countries[user_id]
        
        
        if ( isfraud==True):
            
            amount=random.uniform(5000,20000)
            
        
        else:
            amount=random.uniform(0,5000)
        
        amount=round(amount,2)
            
        if(isCountryFraud==True):
            
            location=random.choice(locations)
            

        payload={
            
            "user_id": user_id,
            "timestamp": time_stamp,
            "merchant_category": merchant,
            "amount": amount,
            "location" : location
        }
        
        
        json_string = json.dumps(payload)
        transaction_data = json_string.encode("utf-8")
        
        producer.send(topic, value=transaction_data)
        
        time.sleep(1)
        
    
except KeyboardInterrupt:
    print("Producer Stop")
    
finally:
    producer.close()
    


