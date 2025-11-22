# Extraction Engine
An engine that can extract the data from image &amp; pdf

## Installation

- [Install Docker](https://www.docker.com/get-started/)
- [Install MongoDB compass](https://www.mongodb.com/docs/compass/install/)

### Local Setup
```bash
python -m venv .venv # Create a virtual environment
pip install -r requirements.txt # Install all the packages

# move .env.example to .env file in workdir
mv .env.example .env

# run minio in docker
docker run -p 9000:9000 -p 9001:9001 quay.io/minio/minio server /data --console-address ":9001"

# Fastapi
fastapi dev app.py

# with workers (optional)
fastapi run --workers 4 main.py

# Uvicorn
# uvicorn <local_file>:app ...
uvicorn app:app --host 0.0.0.0 --port 8080 --workers 4
```

### Docker Setup

Run the following command:
```bash
docker compose up --build # Build & start all the services
docker compose down # Stop the services
```

### Docs
```bash
http://127.0.0.1:8000/docs/ # swagger api docs
```

![Architecture](./assests/images/architecture.png)
