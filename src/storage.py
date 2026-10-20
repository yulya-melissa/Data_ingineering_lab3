"""Работа с RustFS через S3 API."""
from __future__ import annotations

import os
from pathlib import Path
from typing import BinaryIO

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()


def get_s3_client():
    access_key = os.getenv("RUSTFS_ACCESS_KEY")
    secret_key = os.getenv("RUSTFS_SECRET_KEY")
    if not access_key or not secret_key:
        raise RuntimeError("Задайте RUSTFS_ACCESS_KEY и RUSTFS_SECRET_KEY в .env")
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=os.getenv("S3_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
    )


def bucket_name() -> str:
    return os.getenv("S3_BUCKET", "php-vacancies")


def ensure_bucket(client=None, bucket: str | None = None) -> None:
    client = client or get_s3_client()
    bucket = bucket or bucket_name()
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        code = exc.response.get("Error", {}).get("Code", "")
        if status == 404 or code in {"404", "NoSuchBucket", "NotFound"}:
            client.create_bucket(Bucket=bucket)
        else:
            raise


def upload_file(local_path: str | Path, object_key: str) -> None:
    path = Path(local_path)
    if not path.is_file():
        raise FileNotFoundError(f"Локальный файл не найден: {path}")
    client = get_s3_client()
    ensure_bucket(client)
    client.upload_file(str(path), bucket_name(), object_key)
    print(f"Загружено в RustFS: s3://{bucket_name()}/{object_key} ({path.stat().st_size:,} байт)")


def open_object(object_key: str) -> BinaryIO:
    """Возвращает поток тела S3-объекта; вызывающий код должен закрыть поток."""
    response = get_s3_client().get_object(Bucket=bucket_name(), Key=object_key)
    return response["Body"]


def download_file(object_key: str, local_path: str | Path) -> Path:
    path = Path(local_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    get_s3_client().download_file(bucket_name(), object_key, str(path))
    print(f"Скачано из RustFS: s3://{bucket_name()}/{object_key} -> {path}")
    return path


def object_uri(object_key: str) -> str:
    return f"s3://{bucket_name()}/{object_key}"


def list_objects(prefix: str = "") -> list[dict]:
    client = get_s3_client()
    ensure_bucket(client)
    paginator = client.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=bucket_name(), Prefix=prefix):
        objects.extend(page.get("Contents", []))
    return objects
