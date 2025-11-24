from contextlib import asynccontextmanager
from .storage.minio_client import MinioWrapper
from .config import settings
from .db.mongo import get_motor_client, create_collections
from starlette.concurrency import run_in_threadpool


@asynccontextmanager
async def lifespan(app):
    # Clients
    minio_client = MinioWrapper(
        settings.MINIO_ENDPOINT,
        settings.MINIO_ACCESS_KEY,
        settings.MINIO_SECRET_KEY,
        secure=False
    )
    motor_client = get_motor_client(settings.MONGO_URI)

    # Testing MinIO and MongoDB connection
    try:
        await run_in_threadpool(minio_client.client.list_buckets)
        print("MinIO connection successful")

    except Exception as e:
        print(f"MinIO connection failed: {e}")
        raise

    try:
        await motor_client.admin.command("ping")
        print("MongoDB connection successful")
    except Exception as e:
        raise RuntimeError(f"Cannot connect to MongoDB at {settings.MONGO_URI}: {e}")

    meta_coll, jobs_coll = await create_collections(motor_client, settings.MONGO_DB, settings.MONGO_META_COLLECTION, settings.MONGO_JOBS_COLLECTION)

    app.state.minio_client = minio_client
    app.state.motor_client = motor_client
    app.state.meta_coll = meta_coll
    app.state.jobs_coll = jobs_coll

    try:
        yield
    finally:
        motor_client.close()
        pass

