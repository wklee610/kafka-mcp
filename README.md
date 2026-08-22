# Kafka MCP Server
![Python](https://img.shields.io/badge/python-3.10%2B-blue?style=flat-square&logo=python&logoColor=white)
![License](https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square&logo=apache&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-Cluster%20Ops-black?logo=apachekafka)
![MCP](https://img.shields.io/badge/MCP-Compatible-purple)

---

An MCP server implementation for Kafka, allowing LLMs to interact with and manage Kafka clusters.

## Features

- **Cluster Management**: View broker details `describe_cluster`, `describe_brokers`.
- **Topic Management**: List `list_topics`, create `create_topic`, delete `delete_topic`, describe `describe_topic`, and increase partitions `create_partitions`.
- **Configuration Management**: View `describe_configs` and modify `alter_configs` dynamic configs for topics, brokers, and groups.
- **Consumer Groups**: List `list_consumer_groups`, describe `describe_consumer_group`, and securely manage offsets with `reset_consumer_group_offset` and `rewind_consumer_group_offset_by_timestamp`. Advanced tools include state validation, dry runs, and execution audit logging.
- **Messaging**: Consume messages `consume_messages` (from beginning, latest, or specific offsets) and produce messages `produce_message`.
- **Secure Connections**: Connect with TLS/mTLS and SASL PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI, or OAUTHBEARER.
- **JMX Monitoring**: Read broker and topic metrics from one or more Prometheus JMX Exporter endpoints and summarize cluster health.

## Prerequisites

- Python 3.10+
- `uv` package manager (recommended)
- A running Kafka cluster (e.g., local Docker, Confluent Cloud, etc.)

## Installation

1. Clone the repository.
2. Install dependencies:
   ```bash
   uv sync
   ```

## Configuration

Only `KAFKA_BOOTSTRAP_SERVERS` is required. Existing PLAINTEXT configurations remain unchanged.

| Variable | Required | Description |
|----------|----------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | Yes | Comma-separated broker addresses, such as `localhost:9092`. |
| `KAFKA_CLIENT_ID` | No | Kafka client ID. Defaults to `kafka-mcp`. |
| `KAFKA_SECURITY_PROTOCOL` | No | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL`. librdkafka defaults to `PLAINTEXT` when omitted. |
| `KAFKA_CLIENT_CONFIG_JSON` | No | JSON object of additional scalar confluent-kafka/librdkafka client properties. Explicit variables and connection safety settings take precedence. |

### SASL

Set `KAFKA_SASL_MECHANISM` to `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `GSSAPI`, or `OAUTHBEARER` whenever a SASL security protocol is used.

PLAIN and SCRAM use:

```bash
export KAFKA_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_SASL_MECHANISM=SCRAM-SHA-512
export KAFKA_SASL_USERNAME="$KAFKA_USERNAME"
export KAFKA_SASL_PASSWORD="$KAFKA_PASSWORD"
```

GSSAPI/Kerberos supports these optional variables:

| Variable | librdkafka property |
|----------|---------------------|
| `KAFKA_SASL_KERBEROS_SERVICE_NAME` | `sasl.kerberos.service.name` |
| `KAFKA_SASL_KERBEROS_PRINCIPAL` | `sasl.kerberos.principal` |
| `KAFKA_SASL_KERBEROS_KEYTAB` | `sasl.kerberos.keytab` |
| `KAFKA_SASL_KERBEROS_KINIT_CMD` | `sasl.kerberos.kinit.cmd` |
| `KAFKA_SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN` | `sasl.kerberos.min.time.before.relogin` |

The prebuilt `confluent-kafka` wheels do not include GSSAPI. Kerberos deployments outside Docker must install `confluent-kafka` against a librdkafka build compiled with SASL GSSAPI support; the provided Docker image builds that variant from source and includes the Kerberos runtime tools.

OAUTHBEARER supports librdkafka's built-in OIDC flow:

```bash
export KAFKA_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_SASL_MECHANISM=OAUTHBEARER
export KAFKA_SASL_OAUTHBEARER_METHOD=oidc
export KAFKA_SASL_OAUTHBEARER_CLIENT_ID="$OAUTH_CLIENT_ID"
export KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET="$OAUTH_CLIENT_SECRET"
export KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL=https://idp.example.com/oauth/token
export KAFKA_SASL_OAUTHBEARER_SCOPE=kafka
```

`KAFKA_SASL_OAUTHBEARER_EXTENSIONS` is optional. librdkafka's default unsecured-token handler is for development and testing only; it requires both `KAFKA_SASL_OAUTHBEARER_CONFIG` and `KAFKA_ENABLE_SASL_OAUTHBEARER_UNSECURE_JWT=true` instead of the OIDC variables.

Metadata and client-assertion flows are also supported. Set `KAFKA_SASL_OAUTHBEARER_METADATA_AUTHENTICATION_TYPE=azure_imds` for Azure IMDS. AWS IAM uses `aws_iam` with `KAFKA_SASL_OAUTHBEARER_CONFIG`; it additionally requires `confluent-kafka[oauthbearer-aws]` 2.15 or newer. JWT assertion properties such as `sasl.oauthbearer.grant.type` and `sasl.oauthbearer.assertion.private.key.file` can be passed through `KAFKA_CLIENT_CONFIG_JSON`, so the base 2.13 client remains compatible.

### TLS and mTLS

The following variables map directly to librdkafka TLS settings:

| Variable | Description |
|----------|-------------|
| `KAFKA_SSL_CA_LOCATION` | CA certificate path. The system CA store is used when omitted. |
| `KAFKA_SSL_CERTIFICATE_LOCATION` | Client certificate path for mTLS. |
| `KAFKA_SSL_KEY_LOCATION` | Client private key path for mTLS. |
| `KAFKA_SSL_KEY_PASSWORD` | Optional private key password. |
| `KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM` | Hostname verification algorithm, usually `https`. |
| `KAFKA_SSL_CRL_LOCATION` | Certificate revocation list path. |

The client certificate and key must be configured together. Less common librdkafka properties, including PKCS#12 keystores and cipher controls, can be supplied through `KAFKA_CLIENT_CONFIG_JSON`:

```bash
export KAFKA_SECURITY_PROTOCOL=SSL
export KAFKA_CLIENT_CONFIG_JSON='{"ssl.keystore.location":"/run/secrets/client.p12","ssl.keystore.password":"secret"}'
```

### JMX Exporter

JMX monitoring is optional and does not affect the Kafka tools. Run Prometheus JMX Exporter as a Java agent on each broker, then provide its HTTP `/metrics` endpoints. Keeping Java RMI disabled avoids exposing Kafka's unauthenticated remote JMX port.

```bash
export KAFKA_JMX_EXPORTER_ENDPOINTS='{
  "broker-1": "http://broker-1:7071/metrics",
  "broker-2": "http://broker-2:7071/metrics",
  "broker-3": "http://broker-3:7071/metrics"
}'
```

The value may also be a JSON array, a comma-separated URL list, or a comma-separated `name=url` list. For HTTPS or an authenticated reverse proxy, use:

| Variable | Description |
|----------|-------------|
| `KAFKA_JMX_TIMEOUT_SECONDS` | Per-request timeout. Defaults to `5`. |
| `KAFKA_JMX_VERIFY_SSL` | Verify HTTPS certificates. Defaults to `true`. |
| `KAFKA_JMX_CA_LOCATION` | Custom CA certificate path. |
| `KAFKA_JMX_CLIENT_CERTIFICATE_LOCATION` | HTTPS client certificate path. |
| `KAFKA_JMX_CLIENT_KEY_LOCATION` | HTTPS client key path. |
| `KAFKA_JMX_CLIENT_KEY_PASSWORD` | Optional HTTPS client key password. |
| `KAFKA_JMX_USERNAME` / `KAFKA_JMX_PASSWORD` | HTTP Basic authentication. |
| `KAFKA_JMX_BEARER_TOKEN` | HTTP Bearer authentication. |
| `KAFKA_JMX_HEADERS_JSON` | Additional HTTP headers as a JSON object of strings. |

Basic and Bearer authentication are mutually exclusive. JMX tools return a structured `not_configured` response when no exporter endpoints are set.

## Usage

### Running the Server

You can run the server directly using `uv` or `python`, or use Docker.

#### Using uv (Recommended)

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
uv run kafka-mcp
```

#### Using Docker

The image supports TLS, PLAIN, SCRAM, OAUTHBEARER/OIDC, and GSSAPI. Its multi-stage build compiles the Python client against a checksum-verified librdkafka release with Kerberos support.

1. Build the Docker image:
   ```bash
   docker build -t kafka-mcp .
   ```

2. Run the container:
   ```bash
   docker run -i --rm -e KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 kafka-mcp
   ```
   *(Note: Use `host.docker.internal` instead of `localhost` if your Kafka cluster is running on the host machine.)*

### Claude Desktop Configuration

Add the following to your Claude Desktop configuration file (claude_desktop_config.json):

```json
{
  "mcpServers": {
    "kafka": {
      "command": "<uv PATH>",
      "args": [
        "--directory",
        "<kafka-mcp PATH>",
        "run",
        "kafka-mcp"
      ],
      "env": {
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"
      }
    }
  }
}
```


### Debugging / Development

To verify that the server can start and connect to your Kafka cluster (ensure your Kafka is running first):

```bash
# Set your bootstrap server
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Run a quick check
uv run python -c "from src.kafka_mcp import main; print('Imports successful')"
```

### Available Tools

| Category | Tool Name | Description |
|----------|-----------|-------------|
| **Cluster** | `describe_cluster` | Get cluster metadata (controller, brokers). |
| | `describe_brokers` | List all brokers. |
| **Metrics** | `describe_cluster_health` | Summarize replication, ISR, controller, log-directory, and broker reachability signals. |
| | `get_broker_metrics` | Read curated broker metrics or filtered raw exporter samples. |
| | `get_topic_metrics` | Read metrics carrying the requested topic label. |
| **Topics** | `list_topics` | List all available topics. |
| | `describe_topic` | Get detailed info (partitions, replicas) for a topic. |
| | `create_topic` | Create a new topic with partitions/replication factor. |
| | `delete_topic` | Delete a topic. |
| | `create_partitions` | Increase partitions for a topic. |
| **Configs** | `describe_configs` | View dynamic configs for topic/broker/group. |
| | `alter_configs` | Update dynamic configs. |
| **Consumers** | `list_consumer_groups` | List all active consumer groups. |
| | `describe_consumer_group` | Get members and state of a group. |
| | `get_consumer_group_offsets` | Get committed offset, high/low watermarks, and calculate total lag for a topic. |
| | `reset_consumer_group_offset` | Safely change consumer group offsets to earliest, latest, or a specific offset. |
| | `rewind_consumer_group_offset_by_timestamp` | Rewind/advance consumer group offsets securely based on a timestamp. |
| **Messages** | `consume_messages` | Consume messages from a topic (supports offsets, limits). |
| | `produce_message` | Send a message to a topic. |

## Project Structure

```
src/kafka_mcp/
├── configs/       # Configuration handling
├── connections/   # Kafka client factories (singleton)
├── metrics/       # Prometheus JMX Exporter client and parser
├── tools/         # Tool implementations
│   ├── admin.py     # Topic & Config management
│   ├── cluster.py   # Cluster metadata
│   ├── consumer.py  # Consumer group & message consumption
│   ├── metrics.py   # Broker, topic, and cluster health metrics
│   └── producer.py  # Message production
└── main.py        # Entry point & MCP tool registration
```

## Troubleshooting

- **Connection Refused**: Ensure `KAFKA_BOOTSTRAP_SERVERS` is correct and reachable.
- **SASL Authentication Failed**: Verify the security protocol, mechanism, credentials, and CA trust path together.
- **JMX Status Is Unknown**: Ensure the exporter rules include Kafka broker/controller MBeans. Raw samples remain available through `get_broker_metrics(include_raw=true)`.
