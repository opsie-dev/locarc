FROM python:3.13

WORKDIR /locarc

COPY pyproject.toml .
COPY uv.lock .
