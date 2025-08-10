FROM --platform=$TARGETPLATFORM python:3.11-slim@sha256:<ARM64_DIGEST>
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY generate.py .
CMD ["python", "generate.py"]
