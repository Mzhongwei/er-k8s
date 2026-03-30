FROM python:3.10-slim AS builder

ENV PIP_NO_CACHE_DIR=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY ./code/Energy-Aware-Entity-Resolution/requirements-prod.txt /app/requirements.txt

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        python3-dev \
    && python -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install --no-cache-dir -r /app/requirements.txt \
    && /opt/venv/bin/pip install --no-cache-dir kafka-python \
    && rm -rf /var/lib/apt/lists/* /root/.cache/pip

FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY ./code/Energy-Aware-Entity-Resolution/ /app

CMD ["/bin/bash"]