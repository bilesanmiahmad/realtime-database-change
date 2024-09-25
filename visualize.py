from confluent_kafka import Consumer
import json
import matplotlib.pyplot as plt
import datetime
from collections import defaultdict

# Create a Kafka consumer
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'transaction-visualizer',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['cdc.public.transactions'])

# Prepare data structure for visualization
data = defaultdict(list)

# Consume messages and prepare data
message_count = 0
start_time = datetime.datetime.now()
timeout = datetime.timedelta(seconds=30)  # Set a 30-second timeout

try:
    while (datetime.datetime.now() - start_time) < timeout:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Parse the message value
        value = json.loads(msg.value().decode('utf-8'))
        print('Data from poll', value)
        
        # Extract relevant information from the message
        if 'after' in value:
            transaction = value['after']
            timestamp = datetime.datetime.fromtimestamp(transaction['timestamp'])
            amount = transaction['amount']
            currency = transaction['currency']
            
            data[currency].append((timestamp, amount))
            message_count += 1
        
        # Limit to 100 messages
        if message_count >= 100:
            break

    print(f"Collected {message_count} messages.")

except KeyboardInterrupt:
    print("Interrupted by user.")

finally:
    consumer.close()

# Visualize data
plt.figure(figsize=(12, 6))
for currency, transactions in data.items():
    timestamps, amounts = zip(*transactions)
    plt.scatter(timestamps, amounts, label=currency, alpha=0.6)

plt.xlabel('Timestamp')
plt.ylabel('Amount')
plt.title('Transaction Amounts Over Time by Currency')
plt.legend()
plt.gcf().autofmt_xdate()  # Rotate and align the tick labels
plt.show()

# Additional visualization: Transaction count by payment method
payment_methods = defaultdict(int)
for currency, transactions in data.items():
    for timestamp, amount in transactions:
        payment_method = next((t['paymentMethod'] for t in value['after'] if t['amount'] == amount), None)
        if payment_method:
            payment_methods[payment_method] += 1

plt.figure(figsize=(10, 6))
plt.bar(payment_methods.keys(), payment_methods.values())
plt.xlabel('Payment Method')
plt.ylabel('Number of Transactions')
plt.title('Transaction Count by Payment Method')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()