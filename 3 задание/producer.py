from flask import Flask, request, render_template, jsonify
import json
import csv
import io
from kafka import KafkaProducer
from jsonschema import validate, ValidationError
import os
import logging
from datetime import datetime

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = '195.209.210.48:9092'
TOPIC = 'etl_data_topic'

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        acks='all',
        retries=3
    )
    logger.info(f"Подключено к Kafka: {KAFKA_BROKER}")
except Exception as e:
    logger.error(f"Ошибка подключения к Kafka: {e}")
    producer = None

DATA_SCHEMA = {
    "type": "object",
    "properties": {
        "table_name": {"type": "string", "pattern": "^[a-zA-Z_][a-zA-Z0-9_]*$"},
        "columns": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "type": {"type": "string", "enum": ["text", "integer", "float", "boolean", "date"]}
                },
                "required": ["name", "type"]
            }
        },
        "data": {
            "type": "array",
            "items": {"type": "object"}
        }
    },
    "required": ["table_name", "columns"]
}

def save_to_file(data):
    try:
        filename = f"kafka_emulation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"✓ Данные сохранены в файл: {filename}")
        return filename
    except Exception as e:
        logger.error(f"✗ Ошибка сохранения в файл: {e}")
        return None

def validate_csv_data(csv_content, columns=None):
    try:
        csv_file = io.StringIO(csv_content)
        
        delimiter = ','
        if ';' in csv_content[:100]:
            delimiter = ';'
        elif '\t' in csv_content[:100]:
            delimiter = '\t'
        
        reader = csv.DictReader(csv_file, delimiter=delimiter)
        
        data = []
        for row_num, row in enumerate(reader, 1):
            row = {k.strip(): v.strip() if v else v for k, v in row.items()}
            
            if columns and len(columns) != len(row):
                return None, f"Строка {row_num}: количество столбцов не совпадает"
            data.append(row)
        
        return data, None
    except Exception as e:
        return None, f"Ошибка парсинга CSV: {str(e)}"

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/health')
def health():
    kafka_status = "connected" if producer else "disconnected"
    return jsonify({
        "status": "ok",
        "kafka": kafka_status,
        "service": "producer"
    })

@app.route('/send-data', methods=['POST'])
def send_data():
    if not producer:
        logger.error("Kafka producer не доступен")
        return jsonify({'error': 'Kafka producer не доступен'}), 500
    
    try:
        logger.info("Получен запрос на отправку данных")
        
        data_type = request.form.get('data_type', 'manual')
        table_name = request.form['table_name'].strip()
        logger.info(f"Таблица: {table_name}, тип данных: {data_type}")
        
        columns_json = request.form['columns']
        logger.info(f"Столбцы JSON: {columns_json[:200]}...")
        
        try:
            columns = json.loads(columns_json)
            logger.info(f"Столбцы распарсены: {len(columns)} колонок")
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка JSON в столбцах: {e}")
            return jsonify({'error': f'Ошибка в формате JSON столбцов: {str(e)}'}), 400
        
        data = []
        
        if data_type == 'manual':
            data_json = request.form.get('data', '[]')
            logger.info(f"Данные JSON: {data_json[:200]}...")
            
            try:
                data = json.loads(data_json)
                logger.info(f"Данные распарсены: {len(data)} строк")
            except json.JSONDecodeError as e:
                logger.error(f"Ошибка JSON в данных: {e}")
                return jsonify({'error': f'Ошибка в формате JSON данных: {str(e)}'}), 400
            
            if data:
                expected_keys = set(col['name'] for col in columns)
                for i, row in enumerate(data):
                    if not isinstance(row, dict):
                        return jsonify({'error': f'Строка {i}: должна быть объектом'}), 400
                    if set(row.keys()) != expected_keys:
                        return jsonify({
                            'error': f'Строка {i}: ключи не совпадают со столбцами. Ожидалось: {expected_keys}, получено: {set(row.keys())}'
                        }), 400
        
        elif data_type == 'csv':
            if 'csv_file' not in request.files:
                return jsonify({'error': 'CSV файл не загружен'}), 400
            
            csv_file = request.files['csv_file']
            if csv_file.filename == '':
                return jsonify({'error': 'Файл не выбран'}), 400
            
            csv_content = csv_file.read().decode('utf-8-sig')
            
            data, error = validate_csv_data(csv_content, columns)
            if error:
                return jsonify({'error': error}), 400
        
        elif data_type == 'json':
            if 'json_file' not in request.files:
                return jsonify({'error': 'JSON файл не загружен'}), 400
            
            json_file = request.files['json_file']
            if json_file.filename == '':
                return jsonify({'error': 'Файл не выбран'}), 400
            
            data = json.load(json_file)
            
            if not isinstance(data, list):
                return jsonify({'error': 'JSON должен содержать массив объектов'}), 400
            
            expected_keys = set(col['name'] for col in columns)
            for i, row in enumerate(data):
                if not isinstance(row, dict):
                    return jsonify({'error': f'Строка {i}: должна быть объектом'}), 400
                if set(row.keys()) != expected_keys:
                    return jsonify({
                        'error': f'Строка {i}: ключи не совпадают со столбцами'
                    }), 400
        
        message = {
            'table_name': table_name,
            'columns': columns,
            'data': data,
            'operation': 'create_table',
            'timestamp': str(datetime.now())
        }
        
        try:
            validate(instance=message, schema=DATA_SCHEMA)
        except ValidationError as e:
            logger.error(f"Ошибка валидации схемы: {e}")
            return jsonify({'error': f'Ошибка валидации: {e.message}'}), 400
        
        try:
            future = producer.send(TOPIC, message)
            result = future.get(timeout=10)
            
            logger.info(f"✓ Отправлено в Kafka: таблица {table_name}, {len(data)} строк")
            
            return jsonify({
                'success': True,
                'message': f'Данные для таблицы "{table_name}" отправлены в Kafka',
                'details': {
                    'table_name': table_name,
                    'columns': len(columns),
                    'rows': len(data),
                    'mode': 'kafka',
                    'offset': result.offset,
                    'partition': result.partition
                }
            })
        except Exception as e:
            logger.error(f"Ошибка отправки в Kafka: {e}")
            filename = save_to_file(message)
            
            return jsonify({
                'success': True,
                'message': f'Данные для таблицы "{table_name}" сохранены в файл (Kafka недоступен)',
                'details': {
                    'table_name': table_name,
                    'columns': len(columns),
                    'rows': len(data),
                    'mode': 'file',
                    'filename': filename,
                    'note': 'Kafka сервер недоступен, данные сохранены локально'
                }
            })
        
    except Exception as e:
        logger.error(f"✗ Общая ошибка в send_data: {e}", exc_info=True)
        return jsonify({'error': f'Внутренняя ошибка сервера: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000, host='0.0.0.0')