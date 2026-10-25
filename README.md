# Práctica de Big Data: Análisis de Ventas en E-commerce (Batch y Streaming)

Repositorio oficial para la actividad práctica del curso de Big Data (UNAD), basado en un caso de estudio propio de transacciones de ventas en línea procesadas mediante Apache Spark y Apache Kafka.

## Componentes de la Solución
1. **`spark_batch_analysis.py`**: Script de procesamiento en Batch para la carga, limpieza, transformación y análisis exploratorio (EDA) de datos históricos de ventas.
2. **`kafka_producer.py`**: Productor en Python que simula la llegada de transacciones de ventas en tiempo real.
3. **`spark_streaming_consumer.py`**: Aplicación de Spark Streaming que consume los datos del topic de Kafka (`sales_data`) y calcula agregaciones por ventanas de tiempo de 1 minuto.

## Instrucciones de Ejecución

### Procesamiento en Batch:
```bash
python3 spark_batch_analysis.py
Procesamiento en Tiempo Real (Streaming)
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
Crear el topic de Kafka
sudo /opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sales_data
Ejecutar el Productor (Terminal 1)
python3 kafka_producer.py
Ejecutar el Consumidor de Spark (Terminal 2):
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
