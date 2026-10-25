import time
import json
import random
from kafka import KafkaProducer

def generate_sale_data():
    categories = ['Electronica', 'Ropa', 'Hogar', 'Deportes']
    return {
        "product_id": random.randint(100, 200),
        "category": random.choice(categories),
        "price": round(random.uniform(15.0, 600.0), 2),
        "quantity": random.randint(1, 4),
        "timestamp": int(time.time())
    }

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

while True:
    sale_data = generate_sale_data()
    producer.send('sales_data', value=sale_data)
    print(f"Sent: {sale_data}")
    time.sleep(1)
