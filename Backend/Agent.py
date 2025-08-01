import datetime
import time
import subprocess
from pymongo import MongoClient
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import json


print("🚀 MongoDB Trigger Watcher")
print("Database: myDB | Collection: triggers")

# Connect to MongoDB
mongo_uri = "mongodb://localhost:27017/"
db_name = "myDB"  # Replace with your database name
env_collection_name = 'environmentOnboarding'
triggers_collection_name = 'triggers'
client = MongoClient(mongo_uri)
triggers_collection = client[db_name][triggers_collection_name]
Enviroment_Collection = client[db_name][env_collection_name]

def fetch_credentials(mongo_uri, db_name, collection_name, email_to_find, cloud_name, vault_name, secret_name):
    
    # Construct the vault URL
    vault_url = f"https://{vault_name}.vault.azure.net/"

    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Query the collection
    record = collection.find_one({"email": email_to_find, "cloudName": cloud_name})

    if record:
        # Replace with actual credentials
        tenant_id = record['rootId']
        client_id = record['srvaccntName']  # App registered with API permissions
        client_secret = record['srvacctPass']

        # --- Authenticate and fetch secret ---
        credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        kv_client = SecretClient(vault_url=vault_url, credential=credential)
        secret_value = kv_client.get_secret(secret_name).value
        secret_value = secret_value.replace('\\"', '"').replace("'", "")

        # --- Parse JSON and extract fields ---
        secret_json = json.loads(secret_value)
        username = secret_json.get("username")
        password = secret_json.get("password")

        return username, password
    else:
        raise ValueError(f"No record found for email: {email_to_find}")


try:
    while True:
        # Get current timestamp and one minute ago (your exact logic)
        now = datetime.datetime.utcnow().replace(second=0, microsecond=0, tzinfo=datetime.timezone.utc)
        one_minute_ago = now - datetime.timedelta(minutes=1)
        
        # Find triggers matching your exact criteria
        triggers = list(triggers_collection.find({
            "Status": "Pending",
            "ScheduledTimeStamp": {
                "$lte": now
            },
            
            
        }))
        
        
        
        print(f"✅ Updated {len(triggers)} trigger(s) to Completed state")
        # Print message if triggers found
        if triggers:
            print(f"\n🎯 TRIGGER MATCHED! Found {len(triggers)} trigger(s) at {now.isoformat()}")
            for trigger in triggers:
                print(f"   ID: {trigger.get('_id')}")
                print(f"   Scheduled: {trigger.get('ScheduledTimeStamp')}")
                Current_Email = ''
                Current_Email = trigger.get('email')
                
                # Check users collection for CloudName
                Environment_List = Enviroment_Collection.find({"email": Current_Email}).sort("_id")
                # Filter out completed scans
                
                Environment_List = list(Environment_List)

                if Environment_List:
                    for Environment in Environment_List:
                            cloud_name = ''
                            tenant_id = ''
                            client_id =''
                            client_secret= vault_name = secret_name = email_to_find = ''
                            cloud_name = Environment.get('cloudName')
                            tenant_id = Environment.get('rootId')
                            client_id = Environment.get('srvaccntName')  # App registered with API permissions
                            client_secret = Environment.get('srvacctPass')
                            vault_name = Environment.get('vaultname')
                            secret_name = Environment.get('secretname')
                            email_to_find = Environment.get('email')

                            if cloud_name == 'Azure':

                                # Send the Data
                                  # Replace with your MongoDB URI
                                
                                
                          
                                username, password = fetch_credentials(mongo_uri, db_name, env_collection_name, email_to_find, cloud_name, vault_name, secret_name)
                                print(f"Username: {username}")
                                print(f"Password: {password}")
                                print(f"   🔵 Running Azure script")
                                subprocess.run(["python", "Azure.py"])
                            elif cloud_name == 'GCP':
                                print(f"   🟡 Running GCP script")
                                subprocess.run(["python", "Gcp.py"])
                            else:
                                print(f"   ❓ Unknown CloudName: {cloud_name}")
                else:
                    print("  ⚠️ No Environment found in environmentOnboarding collection")
        else:
            print(f"⏳ No triggers found at {now.isoformat()}")
        
        ScanCompletedTime = datetime.datetime.utcnow().replace(second=0, microsecond=0, tzinfo=datetime.timezone.utc)
        
        # Update Triggers to Completed State
        if triggers:
            for trigger in triggers:
                triggers_collection.update_one(
                    {"_id": trigger["_id"]},
                    {"$set": {"Status": "Completed", "ScanCompletedTime": ScanCompletedTime}},
                
            )
            print(f"✅ Updated {len(triggers)} trigger(s) to Completed state")
        # Wait 30 seconds
        time.sleep(30)
        
except KeyboardInterrupt:
    print("\n⚠️ Stopped by user")
#finally:
    #client.close()