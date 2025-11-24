from fastapi import (
    BackgroundTasks,
    FastAPI,
    File,
    UploadFile,
    HTTPException,
    Request,
    Form,
)
import hashlib
from starlette.concurrency import run_in_threadpool
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from minio import Minio
from datetime import datetime, timezone
from minio.error import S3Error
import motor.motor_asyncio
from io import BytesIO
import os
from uuid import uuid4

load_dotenv()

# Configs
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_API_HOST = "http://localhost:9000"
BUCKET = os.getenv("MINIO_BUCKET", "uploads")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "file_db")
COLLECTION = os.getenv("MONGO_COLLECTION", "meta_collection")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Clients
    minio_client = Minio(
        MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
    )
    motor_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)

    # Testing MinIO and MongoDB connection
    try:
        await run_in_threadpool(minio_client.list_buckets)
        print("MinIO connection successful")

    except Exception as e:
        print(f"MinIO connection failed: {e}")
        raise

    try:
        await motor_client.admin.command("ping")
        print("MongoDB connection successful")

    except Exception as e:
        print(f"MongoDB connection failed: {e}")
        raise

    db = motor_client[DB_NAME]
    meta_coll = db[COLLECTION]
    jobs_coll = db["upload_jobs"]

    await meta_coll.create_index("object_name", unique=True)
    await meta_coll.create_index("uploader")
    await meta_coll.create_index("uploaded_at")

    await jobs_coll.create_index("job_id", unique=True)
    await jobs_coll.create_index("status")
    await jobs_coll.create_index("created_at")

    app.state.minio_client = minio_client
    app.state.motor_client = motor_client
    app.state.meta_coll = meta_coll
    app.state.jobs_coll = jobs_coll

    try:
        yield
    finally:
        motor_client.close()
        pass


app = FastAPI(lifespan=lifespan)


@app.post("/upload/")
async def upload(
    request: Request,
    background: BackgroundTasks,
    file: UploadFile = File(...),
    uploader: str | None = Form(None),
):
    """
    Return the job id to the user and consume it for queue
    """
    job_id = uuid4().hex  # Generate the unique job_id
    jobs_coll = request.app.state.jobs_coll
    await jobs_coll.insert_one(
        {
            "job_id": job_id,
            "status": "pending",
            "error": None,
            "result": None,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }
    )

    # Schedule an asyncronous job using background task
    background.add_task(process_upload_job, request, job_id, file, uploader)

    return {
        "job_id": job_id,
        "status": "queued",
    }


async def process_upload_job(
    request: Request,
    job_id: str,
    file: UploadFile = File(...),
    uploader: str | None = Form(None),
):
    minio_client = request.app.state.minio_client
    meta_coll = request.app.state.meta_coll
    jobs_coll = request.app.state.jobs_coll

    # set the job status process
    await jobs_coll.update_one(
        {"job_id": job_id},
        {"$set": {"status": "processing", "updated_at": datetime.now(timezone.utc)}},
    )

    try:
        exists = await run_in_threadpool(minio_client.bucket_exists, BUCKET)
        if not exists:
            await run_in_threadpool(minio_client.make_bucket, BUCKET)

        contents = await file.read()
        checksum = hashlib.sha256(contents).hexdigest()
        stream = BytesIO(contents)  # <- efficient for i/o bound works
        # stream.seek(0) # <- change the stream position to the given bytes

        if not contents:
            raise HTTPException(status_code=404, detail="Empty File")

        if not file.filename:
            raise HTTPException(status_code=404, detail="Missing filename")

        if not file.content_type:
            raise HTTPException(status_code=404, detail="Missing content type")

        object_name = f"{uuid4().hex}_{file.filename}"

        await run_in_threadpool(
            minio_client.put_object,
            BUCKET,
            object_name,
            stream,
            len(contents),
            content_type=file.content_type,
        )

        url = minio_client.presigned_get_object(BUCKET, object_name)
        meta_doc = {
            "filename": file.filename,
            "object_name": object_name,
            "bucket": BUCKET,
            "content_type": file.content_type,
            "size": len(contents),
            "url": url,
            "uploader": uploader,
            "checksum_sha256": checksum,
            "uploaded_at": datetime.now(timezone.utc),
        }

        res = await meta_coll.insert_one(meta_doc)
        meta_doc["_id"] = str(res.inserted_id)

        await jobs_coll.update_one(
            {"job_id": job_id},
            {
                "$set": {
                    "status": "success",
                    "result": meta_doc,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )

    except S3Error as err:
        raise HTTPException(status_code=500, detail=f"MinIO Error: {err}")

    except Exception as e:
        await jobs_coll.update_one(
            {"job_id": job_id},
            {
                "$set": {
                    "status": "failed",
                    "error": str(e),
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )
        print(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail=f"Server Error: {str(e)}")


@app.get("/job/{job_id}")
async def get_job_status(job_id: str, request: Request):
    doc = await request.app.state.jobs_coll.find_one({"job_id": job_id})
    if not doc:
        raise HTTPException(status_code=404, detail="Job not found")

    doc["_id"] = str(doc["_id"])
    return doc
