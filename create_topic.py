#!/usr/bin/env python3
"""
Script to create a Kafka topic on Confluent Cloud with configurable partitions and retention.

This script uses the Confluent Kafka Admin API to create a topic with specified
configuration parameters including number of partitions and retention period.
"""

import os
import sys
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource
from dotenv import load_dotenv


def create_topic(
    bootstrap_servers: str,
    api_key: str,
    api_secret: str,
    topic_name: str,
    num_partitions: int = 6,
    retention_ms: int = 604800000,  # 7 days in milliseconds
    replication_factor: int = 3
):
    """
    Create a Kafka topic on Confluent Cloud.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers URL
        api_key: Confluent Cloud API key
        api_secret: Confluent Cloud API secret
        topic_name: Name of the topic to create
        num_partitions: Number of partitions for the topic (default: 6)
        retention_ms: Retention period in milliseconds (default: 7 days)
        replication_factor: Replication factor (default: 3 for Confluent Cloud)
    
    Returns:
        bool: True if topic was created successfully, False otherwise
    """
    
    # Configure the Admin client
    admin_config = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': api_key,
        'sasl.password': api_secret
    }
    
    admin_client = AdminClient(admin_config)
    
    # Define topic configuration
    topic_config = {
        'retention.ms': str(retention_ms),
        'cleanup.policy': 'delete'
    }
    
    # Create NewTopic object
    new_topic = NewTopic(
        topic=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
        config=topic_config
    )
    
    # Create the topic
    print(f"Creating topic '{topic_name}'...")
    print(f"  Partitions: {num_partitions}")
    print(f"  Retention: {retention_ms}ms ({retention_ms / 86400000:.1f} days)")
    print(f"  Replication Factor: {replication_factor}")
    
    try:
        # Create topics (returns a dict of futures)
        futures = admin_client.create_topics([new_topic])
        
        # Wait for the operation to complete
        for topic, future in futures.items():
            try:
                future.result()  # The result itself is None
                print(f"✓ Topic '{topic}' created successfully!")
                return True
            except Exception as e:
                if "already exists" in str(e).lower():
                    print(f"⚠ Topic '{topic}' already exists.")
                    return False
                else:
                    print(f"✗ Failed to create topic '{topic}': {e}")
                    return False
                    
    except Exception as e:
        print(f"✗ Error creating topic: {e}")
        return False


def main():
    """Main function to load configuration and create the topic."""
    
    # Load environment variables from .env file
    load_dotenv()
    
    # Get required configuration from environment variables
    bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS')
    api_key = os.getenv('KAFKA_API_KEY')
    api_secret = os.getenv('KAFKA_API_SECRET')
    topic_name = os.getenv('TOPIC_NAME', 'inventory.transactions')
    
    # Get optional configuration with defaults
    try:
        num_partitions = int(os.getenv('TOPIC_PARTITIONS', '6'))
    except ValueError:
        print("⚠ Invalid TOPIC_PARTITIONS value, using default: 6")
        num_partitions = 6
    
    try:
        retention_days = float(os.getenv('TOPIC_RETENTION_DAYS', '7'))
        retention_ms = int(retention_days * 86400000)  # Convert days to milliseconds
    except ValueError:
        print("⚠ Invalid TOPIC_RETENTION_DAYS value, using default: 7 days")
        retention_ms = 604800000
    
    try:
        replication_factor = int(os.getenv('TOPIC_REPLICATION_FACTOR', '3'))
    except ValueError:
        print("⚠ Invalid TOPIC_REPLICATION_FACTOR value, using default: 3")
        replication_factor = 3
    
    # Validate required configuration
    if not all([bootstrap_servers, api_key, api_secret]):
        print("✗ Error: Missing required configuration!")
        print("Please ensure the following environment variables are set:")
        print("  - BOOTSTRAP_SERVERS")
        print("  - KAFKA_API_KEY")
        print("  - KAFKA_API_SECRET")
        print("\nYou can set these in a .env file (see .env.example)")
        sys.exit(1)
    
    # Create the topic
    success = create_topic(
        bootstrap_servers=bootstrap_servers,
        api_key=api_key,
        api_secret=api_secret,
        topic_name=topic_name,
        num_partitions=num_partitions,
        retention_ms=retention_ms,
        replication_factor=replication_factor
    )
    
    if success:
        print("\n✓ Topic creation completed successfully!")
        sys.exit(0)
    else:
        print("\n✗ Topic creation failed or topic already exists.")
        sys.exit(1)


if __name__ == "__main__":
    main()

# Made with Bob
