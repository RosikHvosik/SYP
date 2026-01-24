from flask import Flask, request, render_template
import json
import csv
import io
from kafka import KafkaProducer
from datetime import datetime

app = Flask(__name__)

KAFKA_BROKER = '195.209.214.232:9092'
TOPIC = 'etl_data_topic'

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )
    print("Kafka подключен")
except Exception as e:
    print(f"Ошибка Kafka: {e}")
    producer = None

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/send', methods=['POST'])
def send_to_kafka():
    if not producer:
        return "Kafka недоступен", 500

    try:
        table_name = request.form.get('table_name', '').strip()
        if not table_name:
            return "Введите название таблицы", 400

        columns = [
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "text"},
            {"name": "age", "type": "integer"},
            {"name": "score", "type": "float"},
            {"name": "active", "type": "boolean"},
            {"name": "birthday", "type": "date"}
        ]

        data = [
            {"id": 1, "name": "Иван Иванов", "age": 20, "score": 85.5, "active": True, "birthday": "2003-05-15"},
            {"id": 2, "name": "Мария Петрова", "age": 21, "score": 92.0, "active": True, "birthday": "2002-08-22"},
            {"id": 3, "name": "Алексей Сидоров", "age": 22, "score": 78.3, "active": False, "birthday": "2001-11-30"}
        ]

        message = {
            'table_name': table_name,
            'columns': columns,
            'data': data,
            'operation': 'create_table',
            'timestamp': datetime.now().isoformat(),
            'source': 'web_form'
        }

        future = producer.send(TOPIC, message)
        result = future.get(timeout=10)  # ЖДЁМ ПОДТВЕРЖДЕНИЯ

        print(f"Отправлено в Kafka. Offset: {result.offset}")
        return f"Данные отправлены в Kafka. Offset: {result.offset}", 200

    except Exception as e:
        print(f"Ошибка: {e}")
        return f"Ошибка: {str(e)}", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
