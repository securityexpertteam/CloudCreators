import datetime
import time
import subprocess
from pymongo import MongoClient
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import json
from dotenv import load_dotenv
import os
from cryptography.fernet import Fernet
from google.cloud import secretmanager
from google.oauth2 import service_account


# === STEP 1: Conditionally write key to .env ===
def append_key_to_env(env_file=".env", env_key="fernet_key"):
    load_dotenv(dotenv_path=env_file)
    existing_key = os.environ.get(env_key)

    if existing_key:
        print(f"✅ Key '{env_key}' already exists in {env_file}.")
        return existing_key.encode()

    new_key = Fernet.generate_key().decode()

    with open(env_file, "a") as file:
        file.write(f"{env_key}={"'"+new_key+"'"}\n")
        

    print(f"🆕 Fernet key appended to {env_file} as '{env_key}'")
    return new_key.encode()

# === STEP 2: Load key from env ===
def load_key_from_env(env_key="fernet_key"):
    load_dotenv()
    key = os.environ.get(env_key)
    if not key:
        raise ValueError(f"⚠️ Key '{env_key}' not found in environment")
    return key.encode()

# === STEP 3: Encrypt ===
def encrypt_message(message: str, key: bytes) -> bytes:
    fernet = Fernet(key)
    return fernet.encrypt(message.encode())

# === STEP 4: Decrypt ===
def decrypt_message(token: bytes, key: bytes) -> str:
    fernet = Fernet(key)
    return fernet.decrypt(token).decode()


append_key_to_env()
# Load the .env file
load_dotenv()
FERNET_SECRET_KEY = os.getenv("fernet_key")
fernet = Fernet(FERNET_SECRET_KEY)


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

def fetch_credentials(mongo_uri, db_name, collection_name, email_to_find, cloud_name, managementUnit_Id, vault_name, secret_name):
    
    
    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Query the collection
    record = collection.find_one({
        "email": email_to_find,
        "cloudName": cloud_name,
        "managementUnitId": managementUnit_Id
    })


    if record:
        if cloud_name == 'Azure':
            vault_url = f"https://{vault_name}.vault.azure.net/"
            tenant_id = record['rootId']

            # Decrypt client_id (srvaccntName)
            encrypted_client_id = record['srvaccntName']
            try:
                client_id = fernet.decrypt(encrypted_client_id.encode()).decode()
            except Exception as e:
                raise ValueError(f"Decryption failed for client_id: {str(e)}")

            # Decrypt client_secret (srvacctPass)
            encrypted_secret = record['srvacctPass']
            try:
                client_secret = fernet.decrypt(encrypted_secret.encode()).decode()
            except Exception as e:
                raise ValueError(f"Decryption failed for client_secret: {str(e)}")

            credential = ClientSecretCredential(tenant_id, client_id, client_secret)
            kv_client = SecretClient(vault_url=vault_url, credential=credential)
            secret_value = kv_client.get_secret(secret_name).value
            secret_value = secret_value.replace('\\"', '"').replace("'", "")

            secret_json = json.loads(secret_value)
            username = secret_json.get("username")
            password = secret_json.get("password")

            return username, password
        
        elif cloud_name == 'GCP':
            # Step 1: Authenticate using a local JSON key with Secret Manager access
            #AUTH_JSON_PATH = "Creds//pro-plasma-465515-k1-273ccacfba4c.json"
           
            project_id = managementUnit_Id
            AUTH_JSON_PATH = f"Creds//{project_id}.json"

            # 🔒 This key must have secretAccessor role
            if not os.access(AUTH_JSON_PATH, os.R_OK):
                print("No GCP Creds file found")

            credentials = service_account.Credentials.from_service_account_file(AUTH_JSON_PATH)

            # Step 2: Initialize Secret Manager client
            client = secretmanager.SecretManagerServiceClient(credentials=credentials)

            # Step 3: Define secret name
            
            #secret_id = "mygcpvaultreader1-json"
            version_id = "latest"
            #my-gcpsrv1
            secret_name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
 
            # Step 4: Access the secret payload
            response = client.access_secret_version(request={"name": secret_name})
            secret_payload = response.payload.data.decode("UTF-8")

            # Step 5: Parse and use the service account JSON key
            service_account_credentials = json.loads(secret_payload)
            creds = service_account_credentials
            client_email = creds.get("client_email")
            private_key= creds.get("private_key")
            project_id = creds.get("project_id")
          
    
            return project_id,client_email,private_key

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
                            client_secret= username= password =vault_name = secret_name = email_to_find = ''
                            cloud_name = Environment.get('cloudName')
                            tenant_id = Environment.get('rootId')
                            managementUnit_Id = Environment.get('managementUnitId')
                            client_id = Environment.get('srvaccntName')  # App registered with API permissions
                            client_secret = Environment.get('srvacctPass')
                            vault_name = Environment.get('vaultname')
                            secret_name = Environment.get('secretname')
                            email_to_find = Environment.get('email')
                            #print(f"Username: {username}")
                            #print(f"Password: {password}")
                            
                            if cloud_name == 'Azure':

                               
                          
                                print(f" 🔵 Running Azure script")
                                
                                # Send the Data
                                username, password = fetch_credentials(mongo_uri, db_name, env_collection_name, email_to_find, cloud_name,managementUnit_Id,  vault_name, secret_name)
                           
                              
                                cmd = [
                                        "python", "Azure.py",
                                        "--client_id", username,
                                        "--client_secret", password,
                                        "--subscription_id", managementUnit_Id,
                                        "--email", email_to_find,
                                        "--tenant_id", tenant_id,
                                    ]
                              
                                result = subprocess.run(cmd, capture_output=True, text=True)
                                
                            elif cloud_name == 'GCP':
                                print(f"   🟡 Running GCP script")
                                
                                # Send the Data
                                project_id, client_email,private_key = fetch_credentials(mongo_uri, db_name, env_collection_name, email_to_find, cloud_name,managementUnit_Id,  vault_name, secret_name)
                           
                                cmd = [
                                        "python", "Gcp.py",
                                        "--client_email", client_email,
                                        "--private_key", private_key,
                                        "--project_id", project_id,
                                        "--user_email", email_to_find
                                    ]
                                result = subprocess.run(cmd, capture_output=True, text=True)
                                print(result.stdout)
                                print(result.stderr)
                               
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
