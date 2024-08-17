import os
import uuid
import requests

from bson import ObjectId
from niconico import NicoNico
from pymongo import MongoClient
from redis.client import Redis

MONGO_URL = os.environ.get('MONGO_URL')
REDIS_URL = os.environ.get('REDIS_URL')
NICONICO_MAIL = os.environ.get('NICONICO_MAIL')
NICONICO_PASSWORD = os.environ.get('NICONICO_PASSWORD')

if not MONGO_URL:
    raise ValueError('MONGO_URL is not set')
if not REDIS_URL:
    raise ValueError('REDIS_URL is not set')
if not NICONICO_MAIL:
    raise ValueError('NICONICO_MAIL is not set')
if not NICONICO_PASSWORD:
    raise ValueError('NICONICO_PASSWORD is not set')

if os.path.exists("/contents") is False:
    os.makedirs("/contents")
if os.path.exists("/contents/image") is False:
    os.makedirs("/contents/image")
if os.path.exists("/contents/image/icon") is False:
    os.makedirs("/contents/image/icon")
if os.path.exists("/contents/image/thumbnail") is False:
    os.makedirs("/contents/image/thumbnail")
if os.path.exists("/contents/video") is False:
    os.makedirs("/contents/video")

niconico_client = NicoNico()
niconico_client.login_with_mail(NICONICO_MAIL, NICONICO_PASSWORD)

redis_client = Redis.from_url(REDIS_URL)

mongo_client = MongoClient(MONGO_URL)
mongo_db = mongo_client.get_database("nicoarch")
mongo_tasks = mongo_db.get_collection("Task")
mongo_videos = mongo_db.get_collection("Video")
mongo_users = mongo_db.get_collection("User")

def fetch(task_id):
    task = mongo_tasks.find_one_and_update({
        "_id": ObjectId(task_id)
    }, {"$set": {
        "status": "fetching"
    }}, return_document=True)
    watchId = task.get("watchId")
    watchUUID = uuid.uuid3(uuid.NAMESPACE_URL, watchId)
    watchData = niconico_client.video.watch.get_watch_data(watchId)
    userData = niconico_client.user.get_user(str(watchData.owner.id_))
    ownerId = None
    if userData is not None:
        userUUID = uuid.uuid3(uuid.NAMESPACE_URL, userData.id_)
        user_res = mongo_users.insert_one({
            "userId": userData.id_,
            "nickname": userData.nickname,
            "description": userData.description,
            "registeredVersion": userData.registered_version,
            "contentId": str(userUUID)
        })
        with open(f'/contents/image/icon/{str(userUUID)}.jpg', 'wb') as f:
            b = requests.get(userData.icons.large)
            f.write(b.content)
        ownerId = user_res.inserted_id
    video = mongo_videos.insert_one({
        "title": watchData.video.title,
        "watchId": watchId,
        "registeredAt": watchData.video.registered_at,
        "count": {
            "view": watchData.video.count.view,
            "comment": watchData.video.count.comment,
            "mylist": watchData.video.count.mylist,
            "like": watchData.video.count.like
        },
        "ownerId": ownerId,
        "duration": watchData.video.duration,
        "description": watchData.video.description,
        "taskId": ObjectId(task_id),
        "contentId": str(watchUUID)
    })
    return watchData, watchUUID, video.inserted_id

def download(task_id, watchData, watchUUID, videoId):
    mongo_tasks.find_one_and_update({
        "_id": ObjectId(task_id)
    }, {"$set": {
        "status": "downloading",
        "videoId": videoId
    }})
    with open(f'/contents/image/thumbnail/{str(watchUUID)}.jpg', 'wb') as f:
        b = requests.get(watchData.thumbnail.url)
        f.write(b.content)
    outputs = niconico_client.video.watch.get_outputs(watchData)
    best_output = next(iter(outputs))
    niconico_client.video.watch.download_video(watchData, best_output, "/contents/video/"+str(watchUUID)+".%(ext)s")

def finish(task_id):
    mongo_tasks.find_one_and_update({
        "_id": ObjectId(task_id)
    }, {"$set": {
        "status": "completed"
    }})

def error(task_id, e):
    mongo_tasks.find_one_and_update({
        "_id": ObjectId(task_id)
    }, {"$set": {
        "status": "failed",
        "error": str(e)
    }})

def main():
    while True:
        task_id = redis_client.brpop("tasks")[1]
        task_id = task_id.decode('utf-8')
        print(f"Processing task {task_id}")
        try:
            watchData, watchUUID, videoId = fetch(task_id)
            download(task_id, watchData, watchUUID, videoId)
            finish(task_id)
        except Exception as e:
            error(task_id, e)

if __name__ == "__main__":
    main()
