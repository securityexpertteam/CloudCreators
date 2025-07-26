import datetime
import time
import subprocess
from pymongo import MongoClient

print("🚀 MongoDB Trigger Watcher")
print("Database: myDB | Collection: triggers")

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
triggers_collection = client['myDB']['triggers']
users_collection = client['myDB']['users']
    
try:
    while True:
        # Get current timestamp and one minute ago (your exact logic)
        now = datetime.datetime.utcnow().replace(second=0, microsecond=0, tzinfo=datetime.timezone.utc)
        one_minute_ago = now - datetime.timedelta(minutes=1)
        
        # Find triggers matching your exact criteria
        triggers = list(triggers_collection.find({
            "Status": "Pending",
            "ScheduledTimeStamp": {
                "$lte": now,
                "$gt": one_minute_ago
            }
        }))
        
        # Print message if triggers found
        if triggers:
            print(f"\n🎯 TRIGGER MATCHED! Found {len(triggers)} trigger(s) at {now.isoformat()}")
            for trigger in triggers:
                print(f"   ID: {trigger.get('_id')}")
                print(f"   Scheduled: {trigger.get('ScheduledTimeStamp')}")
                
                # Check users collection for CloudName
                user = users_collection.find().sort("_id", -1).limit(1)[0]
                cloud_name = user.get('CloudName')
                if cloud_name == 'Azure':
                    print(f"   🔵 Running Azure script")
                    subprocess.run(["python", r"C:\Users\dasar\OneDrive\Documents\cloud_optimisation\az.py"])
                elif cloud_name == 'GCP':
                    print(f"   🟡 Running GCP script")
                    subprocess.run(["python", r"C:\Users\dasar\OneDrive\Documents\cloud_optimisation\gcp.py"])
                else:
                    print(f"   ❓ Unknown CloudName: {cloud_name}")
        else:
            print(f"⏳ No triggers found at {now.isoformat()}")
        
        # Wait 30 seconds
        time.sleep(30)
        
except KeyboardInterrupt:
    print("\n⚠️ Stopped by user")
finally:
    client.close()
