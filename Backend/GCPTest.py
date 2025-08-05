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

try:
    # Step 1: Authenticate using a local JSON key with Secret Manager access
    AUTH_JSON_PATH = "Backend//Creds//pro-plasma-465515-k1-273ccacfba4c.json"  # 🔒 This key must have secretAccessor role
    credentials = service_account.Credentials.from_service_account_file(AUTH_JSON_PATH)

    # Step 2: Initialize Secret Manager client
    client = secretmanager.SecretManagerServiceClient(credentials=credentials)
    managementUnit_Id = "pro-plasma-465515-k1"
    # Step 3: Define secret name
    project_id = managementUnit_Id
    project_id = "pro-plasma-465515-k1"
    #secret_id = "mygcpvaultreader1-json"
    version_id = "latest"
    secret_name = "my-gcpsrv1"
    secret_name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"

    # Step 4: Access the secret payload
    response = client.access_secret_version(request={"name": secret_name})
    secret_payload = response.payload.data.decode("UTF-8")
    
    creds = json.loads(secret_payload)
    
    private_key = creds.get("private_key")

    client_email = creds.get("client_email")
    private_key= creds.get("private_key")
    project_id = creds.get("project_id")
    email_to_find = "subhash@gmail.com"

    # Step 5: Parse and use the service account JSON key
    service_account_credentials = json.loads(secret_payload)

    print(project_id,client_email,private_key)
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
                                
                           
        
except KeyboardInterrupt:
    print("\n⚠️ Stopped by user")
#finally:

    #client.close()
with open("Backend//Creds//pro-plasma-465515-k1-273ccacfba4c.json" , "r") as f:
    creds = json.load(f)
private_key = creds.get("private_key")
client_email = creds.get("client_email")
private_key= creds.get("private_key")
project_id = creds.get("project_id")
email_to_find = "subhash@gmail.com"
print(project_id,client_email,private_key,email_to_find)