FROM python:3.11-slim-bookworm AS builder

ARG LIBRDKAFKA_VERSION=2.13.0
ARG LIBRDKAFKA_SHA256=3bd351601d8ebcbc99b9a1316cae1b83b00edbcf9411c34287edf1791c507600
ARG CONFLUENT_KAFKA_VERSION=2.13.0

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    libcurl4-openssl-dev \
    liblz4-dev \
    libsasl2-dev \
    libssl-dev \
    libzstd-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL \
        "https://github.com/confluentinc/librdkafka/archive/refs/tags/v${LIBRDKAFKA_VERSION}.tar.gz" \
        -o /tmp/librdkafka.tar.gz \
    && echo "${LIBRDKAFKA_SHA256}  /tmp/librdkafka.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/librdkafka.tar.gz -C /tmp \
    && cd "/tmp/librdkafka-${LIBRDKAFKA_VERSION}" \
    && ./configure --prefix=/usr/local \
    && make -j"$(nproc)" \
    && make install \
    && ldconfig \
    && rm -rf /tmp/librdkafka.tar.gz "/tmp/librdkafka-${LIBRDKAFKA_VERSION}"

WORKDIR /app

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

COPY pyproject.toml README.md ./
COPY src/ ./src/

RUN pip install --no-cache-dir --upgrade pip \
    && printf 'confluent-kafka==%s\n' "${CONFLUENT_KAFKA_VERSION}" > /tmp/constraints.txt \
    && PIP_CONSTRAINT=/tmp/constraints.txt \
        pip install --no-cache-dir --no-binary confluent-kafka . \
    && rm /tmp/constraints.txt


FROM python:3.11-slim-bookworm AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:${PATH}"

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
        --no-install-recommends \
    ca-certificates \
    krb5-user \
    libcurl4 \
    liblz4-1 \
    libsasl2-modules-gssapi-mit \
    libssl3 \
    libzstd1 \
    zlib1g \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/librdkafka.so* /usr/local/lib/
COPY --from=builder /opt/venv /opt/venv

RUN ldconfig

WORKDIR /app

ENTRYPOINT ["kafka-mcp"]
