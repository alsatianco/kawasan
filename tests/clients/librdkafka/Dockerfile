FROM python:3.11-slim-bookworm
ARG CLIENT_VERSION
RUN pip install --no-cache-dir --only-binary=:all: confluent-kafka==${CLIENT_VERSION}
WORKDIR /work
COPY matrix.py .
CMD ["python", "matrix.py"]
