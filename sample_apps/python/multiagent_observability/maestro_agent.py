#!/usr/bin/env python3

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
Orchestrator Agent Script

A production-ready script for running an orchestrator agent with telemetry
and A2A client tools.
"""

import argparse
import logging
import os
import sys
import time
from typing import List, Optional
import httpx

from strands import Agent
from strands.telemetry import StrandsTelemetry
from strands_tools.a2a_client import A2AClientToolProvider


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Configure logging with proper formatting."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    return logging.getLogger(__name__)


def setup_telemetry(endpoint: Optional[str] = None) -> StrandsTelemetry:
    """Setup telemetry with configurable endpoint."""
    if endpoint:
        os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = endpoint
    
    telemetry = StrandsTelemetry()
    telemetry.setup_otlp_exporter()
    return telemetry


def create_agent(
    agent_urls: List[str],
    model: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
    name: str = "Orchestrator Agent"
) -> Agent:
    """Create and configure the orchestrator agent."""
    provider = A2AClientToolProvider(known_agent_urls=agent_urls)
    
    return Agent(
        name=name,
        model=model,
        tools=provider.tools,
    )


def execute_prompt(agent: Agent, prompt: str, logger: logging.Logger) -> str:
    """Execute the prompt and return response with timing."""
    logger.info(f"Executing prompt: {prompt[:100]}{'...' if len(prompt) > 100 else ''}")

    system_instruction = "your sole responsibility is to delegate to other agents."
    
    start_time = time.time()
    try:
        response = agent(f"{system_instruction} {prompt}")
        end_time = time.time()
        runtime = end_time - start_time
        
        logger.info("=" * 50)
        logger.info("RESPONSE:")
        logger.info(response)
        logger.info("=" * 50)
        logger.info(f"Runtime: {runtime:.2f} seconds")
        
        return response
        
    except Exception as e:
        logger.error(f"Error executing prompt: {e}")
        raise


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run orchestrator agent with specified prompt",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "prompt",
        help="The prompt to execute"
    )
    
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level (default: INFO)"
    )
    
    parser.add_argument(
        "--telemetry-endpoint",
        default="http://host.docker.internal:4318",
        help="OTLP telemetry endpoint (default: http://host.docker.internal:4318)"
    )
    
    parser.add_argument(
        "--agent-urls",
        nargs="+",
        default=[
            "http://host.docker.internal:9001",
            "http://host.docker.internal:9002",
        ],
        help="List of known agent URLs"
    )
    
    parser.add_argument(
        "--model",
        default="us.anthropic.claude-3-7-sonnet-20250219-v1:0",
        help="Model to use for the agent"
    )
    
    parser.add_argument(
        "--agent-name",
        default="Orchestrator Agent",
        help="Name for the agent"
    )
    
    return parser.parse_args()


def main() -> int:
    """Main entry point."""
    try:
        args = parse_arguments()
        logger = setup_logging(args.log_level)
        
        logger.info("Starting Orchestrator Agent")
        logger.info(f"Model: {args.model}")
        logger.info(f"Agent URLs: {args.agent_urls}")
        
        # Setup telemetry
        setup_telemetry(args.telemetry_endpoint)
        logger.info(f"Telemetry endpoint: {args.telemetry_endpoint}")
        
        # Create agent
        agent = create_agent(
            agent_urls=args.agent_urls,
            model=args.model,
            name=args.agent_name
        )
        
        # Execute prompt
        execute_prompt(agent, args.prompt, logger)
        
        logger.info("Orchestrator Agent completed successfully")
        return 0
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
