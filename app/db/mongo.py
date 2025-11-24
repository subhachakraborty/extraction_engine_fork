import motor.motor_asyncio

def get_motor_client(mongo_uri: str) -> motor.motor_asyncio.AsyncIOMotorClient:
    return  motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)

async def create_collections(motor_client, db_name: str, meta_collection: str, jobs_collection: str):
    db = motor_client[db_name]
    meta_coll = db[meta_collection]
    jobs_coll = db[jobs_collection]

    await meta_coll.create_index("object_name", unique=True)
    await meta_coll.create_index("uploader")
    await meta_coll.create_index("uploaded_at")

    await jobs_coll.create_index("job_id", unique=True)
    await jobs_coll.create_index("status")
    await jobs_coll.create_index("created_at")

    return meta_coll, jobs_coll
