import logging
import sys
from fastmcp import FastMCP
from .tools.admin import (
    list_topics,
    describe_topic, 
    create_topic, 
    delete_topic, 
    create_partitions,
    describe_configs, 
    alter_configs
)
from .tools.cluster import (
    describe_cluster, 
    describe_brokers
)
from .tools.consumer import (
    consume_messages, 
    list_consumer_groups, 
    describe_consumer_group,
    reset_consumer_group_offset,
    rewind_consumer_group_offset_by_timestamp,
    get_consumer_group_offsets
)
from .tools.producer import produce_message

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize FastMCP
mcp = FastMCP("MCP server for Kafka")

# Admin Tools
mcp.tool()(describe_cluster)
mcp.tool()(describe_brokers)
mcp.tool()(list_topics)
mcp.tool()(describe_topic)
mcp.tool()(create_topic)
mcp.tool()(delete_topic)
mcp.tool()(create_partitions)
mcp.tool()(describe_configs)
mcp.tool()(alter_configs)

# Consumer Tools
mcp.tool()(consume_messages)
mcp.tool()(list_consumer_groups)
mcp.tool()(describe_consumer_group)
mcp.tool()(get_consumer_group_offsets)
mcp.tool()(reset_consumer_group_offset)
mcp.tool()(rewind_consumer_group_offset_by_timestamp)

# Producer Tools
mcp.tool()(produce_message)

def main():
    try:
        mcp.run()
    except Exception as e:
        logging.error(f"Failed to run MCP server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
