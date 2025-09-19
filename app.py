from flask import Flask, jsonify, request, render_template, send_from_directory
from flask_cors import CORS
import mysql.connector
from datetime import datetime, timedelta
import json
from decimal import Decimal
import os

app = Flask(__name__, 
            static_folder='static',
            template_folder='templates')
CORS(app)  # Enable CORS for frontend access

# Database configuration - UPDATE THESE VALUES
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'Goai@2025',
    'database': 'rkmonitor',
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci'
}

class DecimalEncoder(json.JSONEncoder):
    """JSON encoder for Decimal objects"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DecimalEncoder, self).default(obj)

def get_db_connection():
    """Get database connection with proper error handling"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except mysql.connector.Error as e:
        print(f"Database connection error: {e}")
        return None

# Serve the main HTML page
@app.route('/')
def index():
    """Serve the main monitoring dashboard"""
    return render_template('index.html')

# Serve static files (if needed)
@app.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory('static', filename)

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    connection = get_db_connection()
    db_status = 'connected' if connection else 'disconnected'
    
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            connection.close()
        except:
            db_status = 'error'
    
    return jsonify({
        'status': 'healthy',
        'database': db_status,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/devices', methods=['GET'])
def get_devices():
    """Get list of all devices with their status"""
    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Database connection failed', 'devices': []}), 500
    
    try:
        cursor = connection.cursor(dictionary=True)
        
        # First, check if processed tables exist
        cursor.execute("SHOW TABLES")
        tables = [table['Tables_in_rkmonitor'] for table in cursor.fetchall()]
        
        devices_info = []
        
        # Check device 40377991
        if 'device_40377991_processed' in tables:
            query = """
            SELECT 
                40377991 as DeviceAddr,
                'Comprehensive Sensor' as DeviceName,
                COUNT(*) as record_count,
                MIN(RecordTime) as first_record,
                MAX(RecordTime) as last_record,
                AVG(Temperature) as avg_temperature
            FROM device_40377991_processed
            """
            cursor.execute(query)
            device_data = cursor.fetchone()
            if device_data:
                devices_info.append(device_data)
        
        # Check device 40372539
        if 'device_40372539_processed' in tables:
            query = """
            SELECT 
                40372539 as DeviceAddr,
                'Temperature & Velocity Sensor' as DeviceName,
                COUNT(*) as record_count,
                MIN(RecordTime) as first_record,
                MAX(RecordTime) as last_record,
                AVG(Temperature_C) as avg_temperature
            FROM device_40372539_processed
            """
            cursor.execute(query)
            device_data = cursor.fetchone()
            if device_data:
                devices_info.append(device_data)
        
        # If no processed tables, try tbhistory
        if not devices_info and 'tbhistory' in tables:
            query = """
            SELECT DISTINCT 
                DeviceAddr, 
                COALESCE(DeviceName, CONCAT('Device ', DeviceAddr)) as DeviceName,
                COUNT(*) as record_count,
                MIN(RecordTime) as first_record,
                MAX(RecordTime) as last_record,
                AVG(Tem) as avg_temperature
            FROM tbhistory 
            WHERE DeviceAddr IN (40377991, 40372539)
            GROUP BY DeviceAddr, DeviceName
            ORDER BY DeviceAddr
            """
            cursor.execute(query)
            devices_info = cursor.fetchall()
        
        return json.dumps({'devices': devices_info}, cls=DecimalEncoder)
        
    except Exception as e:
        print(f"Error in get_devices: {e}")
        return jsonify({'error': str(e), 'devices': []}), 500
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/api/device/<int:device_addr>/processed', methods=['GET'])
def get_processed_device_data(device_addr):
    """Get processed data for a specific device"""
    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = connection.cursor(dictionary=True)
        
        # Get query parameters
        limit = min(request.args.get('limit', 100, type=int), 1000)
        offset = request.args.get('offset', 0, type=int)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        # Check if processed table exists
        cursor.execute("SHOW TABLES")
        tables = [table['Tables_in_rkmonitor'] for table in cursor.fetchall()]
        
        if device_addr == 40377991:
            table_name = "device_40377991_processed"
            if table_name not in tables:
                return jsonify({'error': 'Processed table not found', 'data': [], 'total': 0}), 404
                
            columns = """
                ID, DeviceName, DeviceAddr, Temperature, 
                `X轴振动速度_mm_s`, `Y轴振动速度_mm_s`, `Z轴振动速度_mm_s`,
                `X_displacement_μm`, `Y_displacement_μm`, `Z_displacement_μm`,
                X_acceleration_m_s2, Y_acceleration_m_s2, Z_acceleration_m_s2,
                RecordTime, CoordinateType, Lng, Lat, IsAlarmData, Source
            """
            
        elif device_addr == 40372539:
            table_name = "device_40372539_processed"
            if table_name not in tables:
                return jsonify({'error': 'Processed table not found', 'data': [], 'total': 0}), 404
                
            columns = """
                ID, DeviceName, DeviceAddr, Temperature_C,
                X_velocity_mm_s, Y_velocity_mm_s, Z_velocity_mm_s,
                RecordTime, CoordinateType, Lng, Lat, IsAlarmData, Source
            """
        else:
            return jsonify({'error': 'Unsupported device', 'data': [], 'total': 0}), 400
        
        # Build query
        query = f"SELECT {columns} FROM {table_name} WHERE DeviceAddr = %s"
        params = [device_addr]
        
        # Add date filtering
        if start_date:
            query += " AND RecordTime >= %s"
            params.append(start_date)
        if end_date:
            query += " AND RecordTime <= %s"
            params.append(end_date)
        
        query += " ORDER BY RecordTime DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        cursor.execute(query, params)
        data = cursor.fetchall()
        
        # Get total count
        count_query = f"SELECT COUNT(*) as total FROM {table_name} WHERE DeviceAddr = %s"
        count_params = [device_addr]
        
        if start_date:
            count_query += " AND RecordTime >= %s"
            count_params.append(start_date)
        if end_date:
            count_query += " AND RecordTime <= %s"
            count_params.append(end_date)
            
        cursor.execute(count_query, count_params)
        total_count = cursor.fetchone()['total']
        
        return json.dumps({
            'data': data,
            'total': total_count,
            'limit': limit,
            'offset': offset,
            'device_addr': device_addr
        }, cls=DecimalEncoder)
        
    except Exception as e:
        print(f"Error in get_processed_device_data: {e}")
        return jsonify({'error': str(e), 'data': [], 'total': 0}), 500
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/api/device/<int:device_addr>/statistics', methods=['GET'])
def get_device_statistics(device_addr):
    """Get statistical summary for a device"""
    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = connection.cursor(dictionary=True)
        
        # Check which table to use
        cursor.execute("SHOW TABLES")
        tables = [table['Tables_in_rkmonitor'] for table in cursor.fetchall()]
        
        basic_stats = {}
        node_stats = []
        recent_stats = {}
        
        if device_addr == 40377991 and 'device_40377991_processed' in tables:
            # Statistics from processed table
            query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT DATE(RecordTime)) as days_active,
                SUM(CASE WHEN IsAlarmData = 1 THEN 1 ELSE 0 END) as alarm_count,
                MIN(RecordTime) as first_record,
                MAX(RecordTime) as last_record,
                AVG(Temperature) as avg_temperature,
                MIN(Temperature) as min_temperature,
                MAX(Temperature) as max_temperature
            FROM device_40377991_processed
            WHERE DeviceAddr = %s
            """
            cursor.execute(query, (device_addr,))
            basic_stats = cursor.fetchone()
            basic_stats['node_count'] = 1  # Single processed device
            
        elif device_addr == 40372539 and 'device_40372539_processed' in tables:
            query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT DATE(RecordTime)) as days_active,
                SUM(CASE WHEN IsAlarmData = 1 THEN 1 ELSE 0 END) as alarm_count,
                MIN(RecordTime) as first_record,
                MAX(RecordTime) as last_record,
                AVG(Temperature_C) as avg_temperature,
                MIN(Temperature_C) as min_temperature,
                MAX(Temperature_C) as max_temperature
            FROM device_40372539_processed
            WHERE DeviceAddr = %s
            """
            cursor.execute(query, (device_addr,))
            basic_stats = cursor.fetchone()
            basic_stats['node_count'] = 1
            
        elif 'tbhistory' in tables:
            # Fallback to raw data
            query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT NodeId) as node_count,
                SUM(CASE WHEN IsAlarmData = 1 THEN 1 ELSE 0 END) as alarm_count,
                MIN(RecordTime) as first_record,
                MAX(RecordTime) as last_record,
                AVG(Tem) as avg_temperature,
                MIN(Tem) as min_temperature,
                MAX(Tem) as max_temperature
            FROM tbhistory 
            WHERE DeviceAddr = %s
            """
            cursor.execute(query, (device_addr,))
            basic_stats = cursor.fetchone()
        
        if not basic_stats or basic_stats['total_records'] == 0:
            return jsonify({
                'basic_stats': {'total_records': 0, 'node_count': 0, 'alarm_count': 0},
                'node_stats': [],
                'recent_activity': {'recent_records': 0},
                'device_addr': device_addr
            })
        
        # Recent activity (last 24 hours)
        yesterday = datetime.now() - timedelta(days=1)
        
        if device_addr == 40377991 and 'device_40377991_processed' in tables:
            recent_query = """
            SELECT COUNT(*) as recent_records
            FROM device_40377991_processed
            WHERE DeviceAddr = %s AND RecordTime >= %s
            """
        elif device_addr == 40372539 and 'device_40372539_processed' in tables:
            recent_query = """
            SELECT COUNT(*) as recent_records
            FROM device_40372539_processed
            WHERE DeviceAddr = %s AND RecordTime >= %s
            """
        else:
            recent_query = """
            SELECT COUNT(*) as recent_records
            FROM tbhistory 
            WHERE DeviceAddr = %s AND RecordTime >= %s
            """
        
        cursor.execute(recent_query, (device_addr, yesterday))
        recent_stats = cursor.fetchone()
        
        return json.dumps({
            'basic_stats': basic_stats,
            'node_stats': node_stats,
            'recent_activity': recent_stats,
            'device_addr': device_addr
        }, cls=DecimalEncoder)
        
    except Exception as e:
        print(f"Error in get_device_statistics: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/api/device/<int:device_addr>/chart-data', methods=['GET'])
def get_chart_data(device_addr):
    """Get data formatted for charts"""
    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = connection.cursor(dictionary=True)
        
        limit = min(request.args.get('limit', 100, type=int), 500)
        hours = request.args.get('hours', 24, type=int)
        
        # Get data from the last N hours
        time_threshold = datetime.now() - timedelta(hours=hours)
        
        # Check which table exists
        cursor.execute("SHOW TABLES")
        tables = [table['Tables_in_rkmonitor'] for table in cursor.fetchall()]
        
        data = []
        
        if device_addr == 40377991:
            if 'device_40377991_processed' in tables:
                query = """
                SELECT Temperature, `X轴振动速度_mm_s`, `Y轴振动速度_mm_s`, `Z轴振动速度_mm_s`,
                       `X_displacement_μm`, `Y_displacement_μm`, `Z_displacement_μm`,
                       X_acceleration_m_s2, Y_acceleration_m_s2, Z_acceleration_m_s2,
                       RecordTime
                FROM device_40377991_processed
                WHERE DeviceAddr = %s AND RecordTime >= %s
                ORDER BY RecordTime DESC
                LIMIT %s
                """
                cursor.execute(query, (device_addr, time_threshold, limit))
                data = cursor.fetchall()
                
        elif device_addr == 40372539:
            if 'device_40372539_processed' in tables:
                query = """
                SELECT Temperature_C, X_velocity_mm_s, Y_velocity_mm_s, Z_velocity_mm_s,
                       RecordTime
                FROM device_40372539_processed
                WHERE DeviceAddr = %s AND RecordTime >= %s
                ORDER BY RecordTime DESC
                LIMIT %s
                """
                cursor.execute(query, (device_addr, time_threshold, limit))
                data = cursor.fetchall()
        
        # If no processed data, try raw data
        if not data and 'tbhistory' in tables:
            query = """
            SELECT NodeId, Tem as Temperature, Hum as Humidity, RecordTime
            FROM tbhistory
            WHERE DeviceAddr = %s AND RecordTime >= %s
            ORDER BY RecordTime DESC
            LIMIT %s
            """
            cursor.execute(query, (device_addr, time_threshold, limit))
            data = cursor.fetchall()
        
        # Reverse to get chronological order for charts
        data.reverse()
        
        return json.dumps({
            'chart_data': data,
            'device_addr': device_addr,
            'time_range_hours': hours,
            'data_points': len(data)
        }, cls=DecimalEncoder)
        
    except Exception as e:
        print(f"Error in get_chart_data: {e}")
        return jsonify({'error': str(e), 'chart_data': []}), 500
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)
    
    print("\n" + "="*60)
    print("Starting Sensor Monitoring System Backend")
    print("="*60)
    print("\nIMPORTANT: Update DB_CONFIG with your database credentials!")
    print("\nPlace your index.html file in the 'templates' folder")
    print("Place any CSS/JS files in the 'static' folder")
    print("\n" + "-"*60)
    print("Server Information:")
    print(f"  URL: http://localhost:5000")
    print(f"  Frontend: http://localhost:5000/")
    print(f"  API Base: http://localhost:5000/api")
    print("-"*60)
    print("\nAvailable API Endpoints:")
    print("  GET /                                      - Main dashboard")
    print("  GET /api/health                           - Health check")
    print("  GET /api/devices                          - List all devices")
    print("  GET /api/device/<device_addr>/processed   - Get processed data")
    print("  GET /api/device/<device_addr>/statistics  - Get device statistics")
    print("  GET /api/device/<device_addr>/chart-data  - Get chart data")
    print("="*60 + "\n")
    
    app.run(host='0.0.0.0', port=5000, debug=True)