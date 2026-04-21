#!/usr/bin/env python3
"""
Script to check the message count in a Kafka topic.
"""

import os
import sys
from confluent_kafka import Consumer, TopicPartition, KafkaException
from dotenv import load_dotenv


def get_message_count(bootstrap_servers: str, api_key: str, api_secret: str, topic_name: str):
    """
    Get the total message count in a topic by checking all partitions.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        api_key: API key
        api_secret: API secret
        topic_name: Topic name
        
    Returns:
        int: Total message count across all partitions
    """
    
    # Configure consumer
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': api_key,
        'sasl.password': api_secret,
        'group.id': 'message-count-checker',
        'auto.offset.reset': 'earliest'
    }
    
    try:
        consumer = Consumer(consumer_config)
        
        # Get metadata for the topic
        metadata = consumer.list_topics(topic_name, timeout=10)
        
        if topic_name not in metadata.topics:
            print(f"❌ Topic '{topic_name}' not found!")
            return None
        
        topic_metadata = metadata.topics[topic_name]
        partitions = topic_metadata.partitions
        
        print(f"📊 Checking message count for topic: {topic_name}")
        print(f"   Number of partitions: {len(partitions)}")
        print()
        
        total_messages = 0
        
        # Check each partition
        for partition_id in partitions.keys():
            # Create TopicPartition for this partition
            tp = TopicPartition(topic_name, partition_id)
            
            # Get low watermark (earliest offset)
            low, high = consumer.get_watermark_offsets(tp, timeout=10)
            
            # Calculate messages in this partition
            partition_messages = high - low
            total_messages += partition_messages
            
            print(f"   Partition {partition_id}:")
            print(f"     Low offset:  {low}")
            print(f"     High offset: {high}")
            print(f"     Messages:    {partition_messages}")
        
        print()
        print("=" * 60)
        print(f"✅ Total messages in topic: {total_messages}")
        print("=" * 60)
        
        consumer.close()
        return total_messages
        
    except KafkaException as e:
        print(f"❌ Kafka error: {e}")
        return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def main():
    """Main function."""
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration
    bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS')
    api_key = os.getenv('KAFKA_API_KEY')
    api_secret = os.getenv('KAFKA_API_SECRET')
    topic_name = os.getenv('TOPIC_NAME', 'inventory.transactions')
    student_id = os.getenv('STUDENT_ID', '')
    
    # Validate configuration
    if not all([bootstrap_servers, api_key, api_secret]):
        print("❌ Error: Missing required configuration!")
        print("Please ensure BOOTSTRAP_SERVERS, KAFKA_API_KEY, and KAFKA_API_SECRET are set in .env")
        sys.exit(1)
    
    print("=" * 60)
    print("🔍 Kafka Topic Message Count Checker")
    print("=" * 60)
    print()
    print(f"Bootstrap Server: {bootstrap_servers}")
    print(f"Topic: {topic_name}")
    if student_id:
        print(f"Student ID: {student_id}")
    print()
    
    # Get message count
    count = get_message_count(bootstrap_servers, api_key, api_secret, topic_name)
    
    if count is not None:
        print()
        if count == 20:
            print("✅ SUCCESS! Expected 20 messages, found 20 messages.")
        elif count > 0:
            print(f"⚠️  Found {count} messages (expected 20)")
        else:
            print("⚠️  No messages found in topic")
        
        sys.exit(0)
    else:
        print("❌ Failed to get message count")
        sys.exit(1)


if __name__ == "__main__":
    main()

# Made with ❤️ by Bob

# Made with Bob
