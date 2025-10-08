from flask import Flask, jsonify, request, render_template
from flask_cors import CORS
import mysql.connector
from datetime import datetime, timezone
import json
from decimal import Decimal
import os
import pytz

app = Flask(__name__,
            static_folder='static',
            template_folder='templates')
CORS(app)

# --- Config ---
CONFIG = {
    'TIMEZONE': 'Asia/Taipei'
}
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'Goai@2025',
    'database': 'rkmonitor',
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci'
}

# --- JSON Encoder for datetime objects ---
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            if obj.tzinfo is None:
                return obj.replace(tzinfo=timezone.utc).isoformat()
            return obj.isoformat()
        return super(DecimalEncoder, self).default(obj)

# --- Database Connection ---
def get_db_connection():
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except mysql.connector.Error as e:
        print(f"Database connection error: {e}")
        return None

# --- NEW HELPER: Logic to process raw data for Device 40377991 ---
def _process_40377991_data(raw_data):
    device_data = [record for record in raw_data if record.get('DeviceAddr') == 40377991]
    if not device_data:
        return []

    device_data.sort(key=lambda x: x['RecordTime'])
    time_groups = {}
    for record in device_data:
        node_id = record.get('NodeId')
        if node_id not in [1, 2, 3, 4, 5]:
            continue
        record_time = record['RecordTime']
        matched_group_key = None
        for key, group_data in time_groups.items():
            if abs((record_time - group_data['base_time']).total_seconds()) <= 60:
                matched_group_key = key
                break
        if matched_group_key:
            time_groups[matched_group_key]['nodes'][node_id] = record
            if record_time < time_groups[matched_group_key]['base_time']:
                time_groups[matched_group_key]['base_time'] = record_time
                time_groups[matched_group_key]['base_record'] = record
        else:
            group_key = f"group_{len(time_groups)}_{record_time.strftime('%Y%m%d%H%M%S')}"
            time_groups[group_key] = {'base_time': record_time, 'base_record': record, 'nodes': {node_id: record}}

    processed_records = []
    for i, (group_key, group_data) in enumerate(time_groups.items()):
        nodes = group_data['nodes']
        base_record = group_data['base_record']
        p = {
            'ID': f"40377991_combined_{base_record['RecordTime'].strftime('%Y%m%d%H%M%S')}_{i:06d}",
            'DeviceName': 'Comprehensive Vibration and Temperature Sensor',
            'DeviceAddr': 40377991,
            'RecordTime': group_data['base_time'],
            'CoordinateType': base_record['CoordinateType'], 'Lng': base_record['Lng'], 'Lat': base_record['Lat'], 'Source': base_record['Source'],
            'IsAlarmData': 0, 'Temperature': None, 'X轴振动速度_mm_s': None, 'Y轴振动速度_mm_s': None, 'Z轴振动速度_mm_s': None,
            'X_displacement_μm': None, 'Y_displacement_μm': None, 'Z_displacement_μm': None, 'X_acceleration_m_s2': None,
            'Y_acceleration_m_s2': None, 'Z_acceleration_m_s2': None
        }
        if 1 in nodes:
            p['Temperature'] = nodes[1]['Hum']
            p['X轴振动速度_mm_s'] = nodes[1]['Tem']
            p['IsAlarmData'] = max(p['IsAlarmData'], nodes[1]['IsAlarmData'])
        if 2 in nodes:
            p['Z轴振动速度_mm_s'] = nodes[2]['Hum']
            p['Y轴振动速度_mm_s'] = nodes[2]['Tem']
            p['IsAlarmData'] = max(p['IsAlarmData'], nodes[2]['IsAlarmData'])
        if 3 in nodes:
            p['Y_displacement_μm'] = nodes[3]['Hum']
            p['X_displacement_μm'] = nodes[3]['Tem']
            p['IsAlarmData'] = max(p['IsAlarmData'], nodes[3]['IsAlarmData'])
        if 4 in nodes:
            p['X_acceleration_m_s2'] = nodes[4]['Hum']
            p['Z_displacement_μm'] = nodes[4]['Tem']
            p['IsAlarmData'] = max(p['IsAlarmData'], nodes[4]['IsAlarmData'])
        if 5 in nodes:
            p['Z_acceleration_m_s2'] = nodes[5]['Hum']
            p['Y_acceleration_m_s2'] = nodes[5]['Tem']
            p['IsAlarmData'] = max(p['IsAlarmData'], nodes[5]['IsAlarmData'])
        processed_records.append(p)
    return processed_records

# --- NEW HELPER: Logic to process raw data for Device 40372539 ---
def _process_40372539_data(raw_data):
    device_data = [record for record in raw_data if record.get('DeviceAddr') == 40372539]
    if not device_data:
        return []

    device_data.sort(key=lambda x: x['RecordTime'])
    time_groups = {}
    for record in device_data:
        node_id = record.get('NodeId')
        if node_id not in [7, 10, 13, 16]:
            continue
        record_time = record['RecordTime']
        matched_group_key = None
        for key, group_data in time_groups.items():
            if abs((record_time - group_data['base_time']).total_seconds()) <= 60:
                matched_group_key = key
                break
        if matched_group_key:
            time_groups[matched_group_key]['nodes'][node_id] = record
            if record_time < time_groups[matched_group_key]['base_time']:
                time_groups[matched_group_key]['base_time'] = record_time
                time_groups[matched_group_key]['base_record'] = record
        else:
            group_key = f"group_{len(time_groups)}_{record_time.strftime('%Y%m%d%H%M%S')}"
            time_groups[group_key] = {'base_time': record_time, 'base_record': record, 'nodes': {node_id: record}}

    processed_records = []
    for i, (group_key, group_data) in enumerate(time_groups.items()):
        nodes = group_data['nodes']
        base_record = group_data['base_record']
        p = {
            'ID': f"40372539_combined_{base_record['RecordTime'].strftime('%Y%m%d%H%M%S')}_{i:06d}",
            'DeviceName': 'Temperature and XYZ Velocity Sensor', 'DeviceAddr': 40372539,
            'RecordTime': group_data['base_time'],
            'CoordinateType': base_record['CoordinateType'], 'Lng': base_record['Lng'], 'Lat': base_record['Lat'], 'Source': base_record['Source'],
            'IsAlarmData': 0, 'Temperature_C': None, 'X_velocity_mm_s': None, 'Y_velocity_mm_s': None, 'Z_velocity_mm_s': None
        }
        if 7 in nodes:
            p['Temperature_C'] = nodes[7]['Tem']
            p['IsAlarmData'] = max(p['IsAlarmData'], nodes[7]['IsAlarmData'])
        if 10 in nodes:
            p['X_velocity_mm_s'] = nodes[10]['Tem']
            p['IsAlarmData'] = max(p['IsAlarmData'], nodes[10]['IsAlarmData'])
        if 13 in nodes:
            p['Y_velocity_mm_s'] = nodes[13]['Tem']
            p['IsAlarmData'] = max(p['IsAlarmData'], nodes[13]['IsAlarmData'])
        if 16 in nodes:
            p['Z_velocity_mm_s'] = nodes[16]['Tem']
            p['IsAlarmData'] = max(p['IsAlarmData'], nodes[16]['IsAlarmData'])
        processed_records.append(p)
    return processed_records

# --- NEW HELPER: Insert processed data into the database ---
def _insert_processed_data(conn, cursor, device_addr, records):
    if not records:
        return 0
    
    if device_addr == 40377991:
        query = """
            INSERT INTO device_40377991_processed (ID, DeviceName, DeviceAddr, Temperature, X轴振动速度_mm_s, Y轴振动速度_mm_s, Z轴振动速度_mm_s, X_displacement_μm, Y_displacement_μm, Z_displacement_μm, X_acceleration_m_s2, Y_acceleration_m_s2, Z_acceleration_m_s2, RecordTime, CoordinateType, Lng, Lat, IsAlarmData, Source)
            VALUES (%(ID)s, %(DeviceName)s, %(DeviceAddr)s, %(Temperature)s, %(X轴振动速度_mm_s)s, %(Y轴振动速度_mm_s)s, %(Z轴振动速度_mm_s)s, %(X_displacement_μm)s, %(Y_displacement_μm)s, %(Z_displacement_μm)s, %(X_acceleration_m_s2)s, %(Y_acceleration_m_s2)s, %(Z_acceleration_m_s2)s, %(RecordTime)s, %(CoordinateType)s, %(Lng)s, %(Lat)s, %(IsAlarmData)s, %(Source)s)
            ON DUPLICATE KEY UPDATE Temperature=VALUES(Temperature), X轴振动速度_mm_s=VALUES(X轴振动速度_mm_s), Y轴振动速度_mm_s=VALUES(Y轴振动速度_mm_s), Z轴振动速度_mm_s=VALUES(Z轴振动速度_mm_s), X_displacement_μm=VALUES(X_displacement_μm), Y_displacement_μm=VALUES(Y_displacement_μm), Z_displacement_μm=VALUES(Z_displacement_μm), X_acceleration_m_s2=VALUES(X_acceleration_m_s2), Y_acceleration_m_s2=VALUES(Y_acceleration_m_s2), Z_acceleration_m_s2=VALUES(Z_acceleration_m_s2), IsAlarmData=VALUES(IsAlarmData)
        """
    elif device_addr == 40372539:
        query = """
            INSERT INTO device_40372539_processed (ID, DeviceName, DeviceAddr, Temperature_C, X_velocity_mm_s, Y_velocity_mm_s, Z_velocity_mm_s, RecordTime, CoordinateType, Lng, Lat, IsAlarmData, Source)
            VALUES (%(ID)s, %(DeviceName)s, %(DeviceAddr)s, %(Temperature_C)s, %(X_velocity_mm_s)s, %(Y_velocity_mm_s)s, %(Z_velocity_mm_s)s, %(RecordTime)s, %(CoordinateType)s, %(Lng)s, %(Lat)s, %(IsAlarmData)s, %(Source)s)
            ON DUPLICATE KEY UPDATE Temperature_C=VALUES(Temperature_C), X_velocity_mm_s=VALUES(X_velocity_mm_s), Y_velocity_mm_s=VALUES(Y_velocity_mm_s), Z_velocity_mm_s=VALUES(Z_velocity_mm_s), IsAlarmData=VALUES(IsAlarmData)
        """
    else:
        return 0
        
    cursor.executemany(query, records)
    conn.commit()
    return cursor.rowcount

# --- REWRITTEN Main sync and fetch function ---
def sync_and_fetch_device_data(device_addr, from_time, to_time, order, limit, offset):
    connection = get_db_connection()
    if not connection:
        return {'error': 'Database connection failed', 'status_code': 500}

    synced, inserted_count = False, 0
    if device_addr == 40377991: table_name = "device_40377991_processed"
    elif device_addr == 40372539: table_name = "device_40372539_processed"
    else: return {'error': 'Unsupported device', 'status_code': 400}

    cursor = connection.cursor(dictionary=True)
    try:
        # 1. Check last processed time for the specific device
        cursor.execute(f"SELECT MAX(RecordTime) as last_ts FROM {table_name} WHERE DeviceAddr = %s", (device_addr,))
        last_processed_naive = (cursor.fetchone() or {}).get('last_ts') or datetime(1970, 1, 1)

        # 2. Check last raw data time
        cursor.execute("SELECT MAX(RecordTime) as last_ts FROM tbhistory WHERE DeviceAddr = %s", (device_addr,))
        last_history_naive = (cursor.fetchone() or {}).get('last_ts') or datetime(1970, 1, 1)

        # 3. If new raw data exists, process and insert it
        if last_history_naive > last_processed_naive:
            synced = True
            print(f"New data found for device {device_addr}. Syncing from {last_processed_naive}...")
            
            cursor.execute("SELECT * FROM tbhistory WHERE DeviceAddr = %s AND RecordTime > %s ORDER BY RecordTime", (device_addr, last_processed_naive))
            new_raw_data = cursor.fetchall()
            
            processed_records = []
            if device_addr == 40377991:
                processed_records = _process_40377991_data(new_raw_data)
            elif device_addr == 40372539:
                processed_records = _process_40372539_data(new_raw_data)
            
            if processed_records:
                inserted_count = _insert_processed_data(connection, cursor, device_addr, processed_records)
                print(f"Sync complete. Inserted/Updated {inserted_count} records for device {device_addr}.")

        # 4. Fetch the data for the requested time range for the frontend
        where = ["DeviceAddr = %s"]
        params = [device_addr]
        if from_time: where.append("RecordTime >= %s"); params.append(from_time)
        if to_time: where.append("RecordTime <= %s"); params.append(to_time)
        
        query = f"SELECT * FROM {table_name} WHERE {' AND '.join(where)} ORDER BY RecordTime {order} LIMIT %s OFFSET %s"
        cursor.execute(query, params + [limit, offset])
        data_rows = cursor.fetchall()
        
        return {'synced': synced, 'inserted': inserted_count, 'rows': data_rows}
    except mysql.connector.Error as e:
        print(f"Error in sync_and_fetch_device_data: {e}")
        return {'error': str(e), 'status_code': 500}
    finally:
        if cursor: cursor.close()
        if connection.is_connected(): connection.close()

# --- Flask Routes ---
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/health', methods=['GET'])
def health_check():
    connection = get_db_connection()
    db_status = 'connected' if connection else 'disconnected'
    if connection:
        try:
            cursor = connection.cursor(); cursor.execute("SELECT 1"); cursor.close(); connection.close()
        except: db_status = 'error'
    return jsonify({'status': 'healthy', 'database': db_status})

@app.route('/api/devices', methods=['GET'])
def get_devices():
    connection = get_db_connection()
    if not connection: return jsonify({'error': 'Database connection failed', 'devices': []}), 500
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SHOW TABLES")
        tables = [table['Tables_in_rkmonitor'] for table in cursor.fetchall()]
        devices_info = []
        if 'device_40377991_processed' in tables:
            devices_info.append({'DeviceAddr': 40377991, 'DeviceName': 'Comprehensive Sensor'})
        if 'device_40372539_processed' in tables:
            devices_info.append({'DeviceAddr': 40372539, 'DeviceName': 'Temperature & Velocity Sensor'})
        return json.dumps({'devices': devices_info}, cls=DecimalEncoder)
    except Exception as e:
        return jsonify({'error': str(e), 'devices': []}), 500
    finally:
        if connection and connection.is_connected(): connection.close()

@app.route('/api/device/<int:device_addr>/data', methods=['GET'])
def get_device_data(device_addr):
    try:
        args = request.args
        limit = int(args.get('limit', 100000)) # Increased default limit
        
        result = sync_and_fetch_device_data(
            device_addr,
            args.get('from'),
            args.get('to'),
            args.get('order', 'desc'),
            limit,
            int(args.get('offset', 0))
        )

        if 'error' in result:
            return jsonify(result), result.get('status_code', 500)
        
        response_data = {
            'data': result.get('rows', []),
            'syncInfo': {
                'synced': result.get('synced'),
                'inserted': result.get('inserted')
            }
        }
        return json.dumps(response_data, cls=DecimalEncoder)
    except Exception as e:
        print(f"Error in get_device_data: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == "__main__":
    if not os.path.exists('templates/index.html'):
        print("Warning: 'templates/index.html' not found.")
    app.run(host='0.0.0.0', port=5000, debug=True)