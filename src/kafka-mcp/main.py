import logging
import sys
from fastmcp import FastMCP
from tools.admin import (
    list_topics,
    describe_topic, 
    create_topic, 
    delete_topic, 
    create_partitions,
    describe_configs, 
    alter_configs
)
from tools.cluster import (
    describe_cluster, 
    describe_brokers
)
from tools.consumer import (
    consume_messages, 
    list_consumer_groups, 
    describe_consumer_group
)
from tools.producer import produce_message

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize FastMCP
mcp = FastMCP("MCP server for Kafka")

# Admin Tools
mcp.add_tool(describe_cluster)
mcp.add_tool(describe_brokers)
mcp.add_tool(list_topics)
mcp.add_tool(describe_topic)
mcp.add_tool(create_topic)
mcp.add_tool(delete_topic)
mcp.add_tool(create_partitions)
mcp.add_tool(describe_configs)
mcp.add_tool(alter_configs)

# Consumer Tools
mcp.add_tool(consume_messages)
mcp.add_tool(list_consumer_groups)
mcp.add_tool(describe_consumer_group)

# Producer Tools
mcp.add_tool(produce_message)

if __name__ == "__main__":
    try:
        mcp.run()
    except Exception as e:
        logging.error(f"Failed to run MCP server: {e}")
        sys.exit(1)
