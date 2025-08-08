import os
from strands.telemetry import StrandsTelemetry
from mcp import stdio_client, StdioServerParameters
from strands import Agent
from strands.tools.mcp import MCPClient
from strands.multiagent.a2a import A2AServer
from contextlib import ExitStack

agent_port = int(os.getenv("AGENT_PORT", 9000))
agent_name = os.getenv("AGENT_NAME", "MCP Expert")
agent_description = os.getenv("AGENT_DESCRIPTION", "An expert with configurable MCP server tools.")
agent_model_id = os.getenv("AGENT_MODEL_ID", "us.amazon.nova-premier-v1:0")
workspace_dir = os.getenv("WORKSPACE_DIR", "/app/output")
aws_region = os.getenv("AWS_REGION", "us-west-2")
http_url = f"http://host.docker.internal:{agent_port}"

# MCP server configuration - comma-separated list of server names
enabled_servers = os.getenv("MCP_SERVERS", "aws_docs,aws_api").split(",")

# initialize OTEL
strands_telemetry = StrandsTelemetry()
strands_telemetry.setup_otlp_exporter()

# Define available MCP clients
mcp_clients = {}

if "aws_docs" in enabled_servers:
    mcp_clients["aws_docs"] = MCPClient(lambda: stdio_client(
        StdioServerParameters(
            command="uvx", 
            args=["awslabs.aws-documentation-mcp-server@latest"]
        )
    ))

if "aws_api" in enabled_servers:
    mcp_clients["aws_api"] = MCPClient(lambda: stdio_client(
        StdioServerParameters(
            command="python", 
            args=["-m","awslabs.aws_api_mcp_server.server"],
            env={
                "AWS_API_MCP_WORKING_DIR": workspace_dir,
                "AWS_REGION": aws_region,
                "READ_OPERATIONS_ONLY": "True",
            }
        )
    ))

if "aws_cfn" in enabled_servers:
    mcp_clients["aws_cfn"] = MCPClient(lambda: stdio_client(
        StdioServerParameters(
            command="uvx", 
            args=["awslabs.cfn-mcp-server@latest","--readonly"]
        )
    ))

# Collect all tools from enabled MCP clients
if mcp_clients:
    with ExitStack() as stack:
        # Enter all MCP clients
        for client in mcp_clients.values():
            stack.enter_context(client)
        
        # Collect tools from all clients
        tools = []
        for client in mcp_clients.values():
            tools.extend(client.list_tools_sync())
        
        agent = Agent(
            name=agent_name,
            description=agent_description,
            model=agent_model_id,
            tools=tools,
            callback_handler=None
        )
        a2a_server = A2AServer(
            agent=agent,
            port=agent_port,
            http_url=http_url
        )
        a2a_server.serve()
else:
    # No MCP clients enabled, create agent with no tools
    agent = Agent(
        name=agent_name,
        description=agent_description,
        model=agent_model_id,
        tools=[],
        callback_handler=None
    )
    a2a_server = A2AServer(agent=agent, port=agent_port)
    a2a_server.serve()
