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
- **Consumer Groups**: List `list_consumer_groups` and describe `describe_consumer_group` consumer groups.
- **Messaging**: Consume messages `consume_messages` (from beginning, latest, or specific offsets) and produce messages `produce_message`.

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

The server requires the `KAFKA_BOOTSTRAP_SERVERS` environment variable.

- `KAFKA_BOOTSTRAP_SERVERS`: Comma-separated list of broker urls (e.g., `localhost:9092`).
- `KAFKA_CLIENT_ID`: (Optional) Client ID for connection (default: `kafka-mcp`).

## Usage

### Running the Server

You can run the server directly using `uv` or `python`.

```bash
# Using uv (Recommended)
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
uv run src/kafka-mcp/main.py
```

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
        "main.py"
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
| **Topics** | `list_topics` | List all available topics. |
| | `describe_topic` | Get detailed info (partitions, replicas) for a topic. |
| | `create_topic` | Create a new topic with partitions/replication factor. |
| | `delete_topic` | Delete a topic. |
| | `create_partitions` | Increase partitions for a topic. |
| **Configs** | `describe_configs` | View dynamic configs for topic/broker/group. |
| | `alter_configs` | Update dynamic configs. |
| **Consumers** | `list_consumer_groups` | List all active consumer groups. |
| | `describe_consumer_group` | Get members and state of a group. |
| **Messages** | `consume_messages` | Consume messages from a topic (supports offsets, limits). |
| | `produce_message` | Send a message to a topic. |

## Project Structure

```
src/kafka-mcp/
├── configs/       # Configuration handling
├── connections/   # Kafka client factories (singleton)
├── tools/         # Tool implementations
│   ├── admin.py     # Topic & Config management
│   ├── cluster.py   # Cluster metadata
│   ├── consumer.py  # Consumer group & message consumption
│   └── producer.py  # Message production
└── main.py        # Entry point & MCP tool registration
```

## Troubleshooting

- **Connection Refused**: Ensure `KAFKA_BOOTSTRAP_SERVERS` is correct and reachable.

## TODO
- SASL
- JMX