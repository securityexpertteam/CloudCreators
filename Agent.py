import datetime
import time
import subprocess
from pymongo import MongoClient

print("🚀 MongoDB Trigger Watcher")
print("Database: myDB | Collection: triggers")

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
triggers_collection = client['myDB']['triggers']
Enviroment_Collection = client['myDB']['environmentOnboarding']

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
                            cloud_name = Environment.get('cloudName')
                            if cloud_name == 'Azure':
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
        
        ScanCompletedTime = datetime.datetime.now(datetime.UTC).replace(second=0, microsecond=0)
        
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