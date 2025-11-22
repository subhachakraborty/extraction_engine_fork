from fastapi import FastAPI, File, UploadFile, HTTPException, Request, Form
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
        minio_client.list_buckets()
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

    await meta_coll.create_index("object_name", unique=True)
    await meta_coll.create_index("uploader")
    await meta_coll.create_index("uploaded_at")

    app.state.minio_client = minio_client
    app.state.motor_client = motor_client
    app.state.meta_coll = meta_coll

    try:
        yield
    finally:
        motor_client.close()
        pass


app = FastAPI(lifespan=lifespan)


# Checksum Generator
def sha256_bytesio(b: bytes) -> str:
    import hashlib

    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


@app.post("/upload/")
async def upload(
    request: Request, file: UploadFile = File(...), uploader: str | None = Form(None)
):
    """
    Upload a file, store to MinIO, insert a submissions doc and enqueue job.
    """
    minio_client = request.app.state.minio_client
    meta_coll = request.app.state.meta_coll

    if not minio_client.bucket_exists(BUCKET):
        minio_client.make_bucket(BUCKET)

    contents = await file.read()
    if not contents:
        raise HTTPException(status_code=404, detail="Empty File")

    data = BytesIO(contents)  # <- efficient for i/o bound works
    # data.seek(0) # <- change the stream position to the given bytes

    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")

    if not file.content_type:
        raise HTTPException(status_code=400, detail="Missing content type")

    object_name = f"{uuid4().hex}_{file.filename}"
    try:
        minio_client.put_object(
            BUCKET,
            object_name,
            data,
            length=len(contents),
            content_type=file.content_type,
        )

        url = minio_client.presigned_get_object(BUCKET, object_name)

        doc = {
            "filename": file.filename,
            "object_name": object_name,
            "bucket": BUCKET,
            "content_type": file.content_type,
            "size": len(contents),
            "url": url,
            "uploader": uploader,
            "checksum_sha256": sha256_bytesio(contents),
            "uploaded_at": datetime.now(timezone.utc),
        }

        res = await meta_coll.insert_one(doc)
        doc["_id"] = str(res.inserted_id)

        return {
            "status": "upload successful",
            "meta": doc,
        }

    except S3Error as err:
        raise HTTPException(status_code=500, detail=f"MinIO Error: {err}")

    except Exception as e:
        print(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail=f"Server Error: {str(e)}")
