from flask import Flask, jsonify, request, render_template, send_from_directory
from flask_cors import CORS
import mysql.connector
from datetime import datetime, timedelta
import json
from decimal import Decimal
import os
import numpy as np
import pandas as pd
from scipy.fft import fft, fftfreq
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

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
    'charset': 'utf8mb4'    ,
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

# Anomaly Analysis Endpoints
@app.route('/api/device/<int:device_addr>/analyze', methods=['POST'])
def analyze_device_anomalies(device_addr):
    """Perform FFT and Isolation Forest analysis on device data"""
    try:
        # Get parameters from request
        hours = request.json.get('hours', 24) if request.json else 24
        contamination = request.json.get('contamination', 0.05) if request.json else 0.05
        
        # Get connection
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = connection.cursor(dictionary=True)
        
        # STEP 1: Update/Refresh data from original source to processed tables
        print(f"Step 1: Updating data for device {device_addr}...")
        
        # Check which tables exist
        cursor.execute("SHOW TABLES")
        tables = [table['Tables_in_rkmonitor'] for table in cursor.fetchall()]
        
        # First, update the processed table from raw data if needed
        if 'tbhistory' in tables:
            if device_addr == 40377991:
                # Update device_40377991_processed from latest raw data
                update_query = """
                INSERT INTO device_40377991_processed 
                SELECT * FROM tbhistory 
                WHERE DeviceAddr = %s 
                AND RecordTime > (
                    SELECT COALESCE(MAX(RecordTime), '1970-01-01') 
                    FROM device_40377991_processed 
                    WHERE DeviceAddr = %s
                )
                ON DUPLICATE KEY UPDATE 
                RecordTime = VALUES(RecordTime)
                """
                try:
                    cursor.execute(update_query, (device_addr, device_addr))
                    connection.commit()
                    print(f"Updated {cursor.rowcount} new records for device {device_addr}")
                except Exception as update_error:
                    print(f"Update warning: {update_error}")
                    
            elif device_addr == 40372539:
                # Update device_40372539_processed from latest raw data
                update_query = """
                INSERT INTO device_40372539_processed 
                SELECT * FROM tbhistory 
                WHERE DeviceAddr = %s 
                AND RecordTime > (
                    SELECT COALESCE(MAX(RecordTime), '1970-01-01') 
                    FROM device_40372539_processed 
                    WHERE DeviceAddr = %s
                )
                ON DUPLICATE KEY UPDATE 
                RecordTime = VALUES(RecordTime)
                """
                try:
                    cursor.execute(update_query, (device_addr, device_addr))
                    connection.commit()
                    print(f"Updated {cursor.rowcount} new records for device {device_addr}")
                except Exception as update_error:
                    print(f"Update warning: {update_error}")
        
        # STEP 2: Fetch the latest updated data for analysis
        print(f"Step 2: Fetching latest data for analysis...")
        
        data = []
        
        # Try to fetch data based on device
        if device_addr == 40377991:
            # Get latest data from processed table
            if 'device_40377991_processed' in tables:
                query = """
                SELECT RecordTime, Temperature,
                       `X轴振动速度_mm_s` as X_vibration, 
                       `Y轴振动速度_mm_s` as Y_vibration, 
                       `Z轴振动速度_mm_s` as Z_vibration,
                       `X_displacement_μm` as X_displacement, 
                       `Y_displacement_μm` as Y_displacement, 
                       `Z_displacement_μm` as Z_displacement,
                       X_acceleration_m_s2, Y_acceleration_m_s2, Z_acceleration_m_s2
                FROM device_40377991_processed
                WHERE DeviceAddr = %s 
                ORDER BY RecordTime DESC
                LIMIT 1000
                """
                cursor.execute(query, (device_addr,))
                data = cursor.fetchall()
                print(f"Retrieved {len(data)} records from processed table")
            
            # If no data in processed table, try raw table
            if not data and 'tbhistory' in tables:
                query = """
                SELECT RecordTime, Tem as Temperature, Hum as Humidity, NodeId
                FROM tbhistory
                WHERE DeviceAddr = %s
                ORDER BY RecordTime DESC
                LIMIT 1000
                """
                cursor.execute(query, (device_addr,))
                data = cursor.fetchall()
                print(f"Retrieved {len(data)} records from raw table")
                
        elif device_addr == 40372539:
            # Get latest data from processed table
            if 'device_40372539_processed' in tables:
                query = """
                SELECT RecordTime, Temperature_C as Temperature,
                       X_velocity_mm_s, Y_velocity_mm_s, Z_velocity_mm_s
                FROM device_40372539_processed
                WHERE DeviceAddr = %s
                ORDER BY RecordTime DESC
                LIMIT 1000
                """
                cursor.execute(query, (device_addr,))
                data = cursor.fetchall()
                print(f"Retrieved {len(data)} records from processed table")
            
            # If no data in processed table, try raw table
            if not data and 'tbhistory' in tables:
                query = """
                SELECT RecordTime, Tem as Temperature, Hum as Humidity, NodeId
                FROM tbhistory
                WHERE DeviceAddr = %s
                ORDER BY RecordTime DESC
                LIMIT 1000
                """
                cursor.execute(query, (device_addr,))
                data = cursor.fetchall()
                print(f"Retrieved {len(data)} records from raw table")
        
        cursor.close()
        connection.close()
        
        if not data:
            # Try to provide more detailed error info
            return jsonify({
                'error': 'No data available for analysis after update',
                'details': f'Device {device_addr} has no records in database',
                'tables_checked': tables,
                'device_addr': device_addr
            }), 404
        
        # STEP 3: Proceed with analysis on the latest data
        print(f"Step 3: Analyzing {len(data)} records...")
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Convert Decimal to float
        for col in df.columns:
            if df[col].dtype == object:
                try:
                    df[col] = pd.to_numeric(df[col], errors='ignore')
                except:
                    pass
        
        # Sort by time for proper analysis
        if 'RecordTime' in df.columns:
            df = df.sort_values('RecordTime')
        
        results = {
            'device_addr': device_addr,
            'analysis_timestamp': datetime.now().isoformat(),
            'data_points': len(df),
            'data_freshness': 'Latest data fetched and analyzed',
            'time_range': {
                'start': str(df['RecordTime'].min()) if 'RecordTime' in df.columns else 'N/A',
                'end': str(df['RecordTime'].max()) if 'RecordTime' in df.columns else 'N/A'
            }
        }
        
        # Perform FFT Analysis - Only for X, Y, Z vibration
        fft_results = {}
        
        # Device-specific FFT based on available columns
        if device_addr == 40377991:
            # Only analyze vibration signals for X, Y, Z
            for axis, col in [('X', 'X_vibration'), ('Y', 'Y_vibration'), ('Z', 'Z_vibration')]:
                if col in df.columns:
                    signal = df[col].fillna(0).values
                    if len(signal) > 10:
                        fft_result = perform_fft_analysis(signal, f'{axis}-axis Vibration')
                        if fft_result:
                            fft_results[f'{axis}_vibration'] = fft_result
            
            # Also check for Chinese column names
            if not fft_results:
                for axis in ['X', 'Y', 'Z']:
                    col_name = f'{axis}轴振动速度_mm_s'
                    if col_name in df.columns:
                        signal = df[col_name].fillna(0).values
                        if len(signal) > 10:
                            fft_result = perform_fft_analysis(signal, f'{axis}-axis Vibration')
                            if fft_result:
                                fft_results[f'{axis}_vibration'] = fft_result
        
        elif device_addr == 40372539:
            # Only analyze velocity signals for X, Y, Z
            for axis in ['X', 'Y', 'Z']:
                col_name = f'{axis}_velocity_mm_s'
                if col_name in df.columns:
                    signal = df[col_name].fillna(0).values
                    if len(signal) > 10:
                        fft_result = perform_fft_analysis(signal, f'{axis}-axis Velocity')
                        if fft_result:
                            fft_results[f'{axis}_velocity'] = fft_result
        
        results['fft_analysis'] = fft_results
        
        # Perform Isolation Forest Analysis
        # Focus on vibration/velocity columns for anomaly detection
        if device_addr == 40377991:
            # Select vibration-related columns
            vibration_cols = []
            for col in df.columns:
                if any(x in col.lower() for x in ['vibration', '振动速度', 'displacement', 'acceleration']):
                    if df[col].dtype in [np.float64, np.float32, np.int64, np.int32]:
                        vibration_cols.append(col)
            
            numeric_cols = vibration_cols if vibration_cols else df.select_dtypes(include=[np.number]).columns.tolist()
        
        elif device_addr == 40372539:
            # Select velocity columns
            velocity_cols = []
            for col in df.columns:
                if 'velocity' in col.lower():
                    if df[col].dtype in [np.float64, np.float32, np.int64, np.int32]:
                        velocity_cols.append(col)
            
            numeric_cols = velocity_cols if velocity_cols else df.select_dtypes(include=[np.number]).columns.tolist()
        else:
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        
        # Remove ID and timestamp columns
        numeric_cols = [col for col in numeric_cols if 'ID' not in col.upper() and 'NODEID' not in col.upper() and 'RECORDTIME' not in col.upper()]
        
        if numeric_cols:
            # Remove ID columns if present
            numeric_cols = [col for col in numeric_cols if 'ID' not in col.upper() and 'NODEID' not in col.upper()]
            
            if numeric_cols:
                X = df[numeric_cols].fillna(0).values
                
                # Only proceed if we have enough data
                if X.shape[0] > 20:
                    # Standardize features
                    scaler = StandardScaler()
                    X_scaled = scaler.fit_transform(X)
                    
                    # Train Isolation Forest
                    iso_forest = IsolationForest(
                        n_estimators=150,
                        contamination=contamination,
                        random_state=42,
                        max_samples=min(256, X.shape[0])
                    )
                    
                    predictions = iso_forest.fit_predict(X_scaled)
                    scores = iso_forest.score_samples(X_scaled)
                    
                    # Get anomaly statistics
                    anomaly_indices = np.where(predictions == -1)[0]
                    normal_indices = np.where(predictions == 1)[0]
                    anomaly_percentage = (len(anomaly_indices) / len(predictions)) * 100
                    
                    results['isolation_forest'] = {
                        'predictions': predictions.tolist(),
                        'anomaly_scores': scores.tolist(),
                        'anomaly_indices': anomaly_indices.tolist(),
                        'normal_indices': normal_indices.tolist(),
                        'anomaly_percentage': float(anomaly_percentage),
                        'total_samples': len(predictions),
                        'total_anomalies': len(anomaly_indices),
                        'feature_columns': numeric_cols,
                        'contamination': contamination,
                        'timestamps': df['RecordTime'].astype(str).tolist() if 'RecordTime' in df.columns else []
                    }
                    
                    # Add anomaly labels to dataframe
                    df['is_anomaly'] = predictions
                    df['anomaly_score'] = scores
        
        # Create fft_analysis folder
        analysis_folder = 'fft_analysis'
        os.makedirs(analysis_folder, exist_ok=True)
        
        # Save results to file with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save JSON analysis results
        json_filename = f'device_{device_addr}_analysis_{timestamp}.json'
        json_filepath = os.path.join(analysis_folder, json_filename)
        
        with open(json_filepath, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, default=str, ensure_ascii=False)
        
        # Save data with anomaly labels as CSV
        csv_filename = f'device_{device_addr}_data_{timestamp}.csv'
        csv_filepath = os.path.join(analysis_folder, csv_filename)
        df.to_csv(csv_filepath, index=False, encoding='utf-8')
        
        # Get latest files in the folder
        latest_files = []
        for file in os.listdir(analysis_folder):
            if file.startswith(f'device_{device_addr}'):
                file_path = os.path.join(analysis_folder, file)
                file_stat = os.stat(file_path)
                latest_files.append({
                    'filename': file,
                    'path': file_path,
                    'size': file_stat.st_size,
                    'created': datetime.fromtimestamp(file_stat.st_ctime).isoformat()
                })
        
        # Sort by creation time
        latest_files.sort(key=lambda x: x['created'], reverse=True)
        
        results['files_saved'] = {
            'analysis_json': json_filepath,
            'data_csv': csv_filepath,
            'folder': analysis_folder,
            'latest_files': latest_files[:5]  # Return 5 most recent files
        }
        
        return jsonify(results)
        
    except Exception as e:
        print(f"Error in analyze_device_anomalies: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'error': str(e),
            'type': type(e).__name__,
            'device_addr': device_addr
        }), 500

def perform_fft_analysis(signal, signal_name="Signal"):
    """Perform FFT analysis on a signal"""
    # Remove NaN values
    clean_signal = signal[~np.isnan(signal)]
    
    if len(clean_signal) < 2:
        return None
    
    # Perform FFT
    N = len(clean_signal)
    yf = fft(clean_signal)
    xf = fftfreq(N, 1.0)[:N//2]
    
    # Calculate magnitude spectrum
    magnitude = 2.0/N * np.abs(yf[:N//2])
    
    # Find dominant frequencies
    threshold = np.max(magnitude) * 0.1
    dominant_freq_indices = np.where(magnitude > threshold)[0]
    dominant_frequencies = xf[dominant_freq_indices].tolist()
    dominant_magnitudes = magnitude[dominant_freq_indices].tolist()
    
    return {
        'signal_name': signal_name,
        'frequencies': xf.tolist(),
        'magnitudes': magnitude.tolist(),
        'dominant_frequencies': dominant_frequencies,
        'dominant_magnitudes': dominant_magnitudes,
        'signal_length': N,
        'max_magnitude': float(np.max(magnitude)),
        'mean_magnitude': float(np.mean(magnitude))
    }

@app.route('/api/analysis-history', methods=['GET'])
def get_analysis_history():
    """Get list of saved analysis files"""
    try:
        files = []
        analysis_folder = 'fft_analysis'
        
        if os.path.exists(analysis_folder):
            for filename in os.listdir(analysis_folder):
                if filename.endswith('.json'):
                    filepath = os.path.join(analysis_folder, filename)
                    stats = os.stat(filepath)
                    files.append({
                        'filename': filename,
                        'path': filepath,
                        'size': stats.st_size,
                        'created': datetime.fromtimestamp(stats.st_ctime).isoformat(),
                        'modified': datetime.fromtimestamp(stats.st_mtime).isoformat()
                    })
        
        files.sort(key=lambda x: x['created'], reverse=True)
        return jsonify({'files': files, 'folder': analysis_folder})
        
    except Exception as e:
        print(f"Error in get_analysis_history: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analysis-file/<filename>', methods=['GET'])
def get_analysis_file(filename):
    """Retrieve a specific analysis file"""
    try:
        analysis_folder = 'fft_analysis'
        filepath = os.path.join(analysis_folder, filename)
        
        # Security check - prevent directory traversal
        if '..' in filename or '/' in filename or '\\' in filename:
            return jsonify({'error': 'Invalid filename'}), 400
        
        if not os.path.exists(filepath):
            return jsonify({'error': 'File not found'}), 404
        
        with open(filepath, 'r', encoding='utf-8') as f:
            content = json.load(f)
        
        return jsonify(content)
        
    except Exception as e:
        print(f"Error reading analysis file: {e}")
        return jsonify({'error': str(e)}), 500

   # data_sync_api.py
# Add these endpoints to your Flask app.py file

@app.route('/api/sync/device/<int:device_addr>', methods=['POST'])
def sync_device_data(device_addr):
    """
    Sync data from tbhistory to device_*_processed tables
    with proper node mapping and time grouping
    """
    try:
        # Get optional parameters
        request_data = request.get_json() if request.is_json else {}
        force_resync = request_data.get('force_resync', False)  # If True, resync all data
        days_back = request_data.get('days_back', 30)  # How many days back to sync
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = connection.cursor(dictionary=True)
        
        sync_result = {
            'device_addr': device_addr,
            'sync_timestamp': datetime.now().isoformat(),
            'source_table': 'tbhistory',
            'target_table': f'device_{device_addr}_processed',
            'records_processed': 0,
            'groups_created': 0,
            'latest_record': None,
            'sync_mode': 'full' if force_resync else 'incremental'
        }
        
        if device_addr == 40377991:
            sync_result.update(sync_device_40377991(cursor, connection, force_resync, days_back))
        elif device_addr == 40372539:
            sync_result.update(sync_device_40372539(cursor, connection, force_resync, days_back))
        else:
            cursor.close()
            connection.close()
            return jsonify({'error': f'Unsupported device: {device_addr}'}), 400
        
        cursor.close()
        connection.close()
        
        return jsonify(sync_result)
        
    except Exception as e:
        print(f"Error in sync_device_data: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'error': str(e),
            'device_addr': device_addr
        }), 500

def sync_device_40377991(cursor, connection, force_resync=False, days_back=30):
    """Sync Device 40377991 data with proper node mapping"""
    
    # Ensure table exists with correct structure
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS device_40377991_processed (
        `ID` varchar(128) PRIMARY KEY,
        `DeviceName` varchar(255) DEFAULT 'Comprehensive Vibration and Temperature Sensor',
        `DeviceAddr` int DEFAULT 40377991,
        `Temperature` double(20,4) COMMENT 'Node 1 Hum field',
        `X轴振动速度_mm_s` double(20,4) COMMENT 'Node 1 Tem field',
        `Y轴振动速度_mm_s` double(20,4) COMMENT 'Node 2 Tem field',
        `Z轴振动速度_mm_s` double(20,4) COMMENT 'Node 2 Hum field',
        `X_displacement_μm` double(20,4) COMMENT 'Node 3 Tem field',
        `Y_displacement_μm` double(20,4) COMMENT 'Node 3 Hum field',
        `X_acceleration_m_s2` double(20,4) COMMENT 'Node 4 Hum field',
        `Z_displacement_μm` double(20,4) COMMENT 'Node 4 Tem field',
        `Y_acceleration_m_s2` double(20,4) COMMENT 'Node 5 Tem field',
        `Z_acceleration_m_s2` double(20,4) COMMENT 'Node 5 Hum field',
        `RecordTime` datetime,
        `CoordinateType` tinyint(1),
        `Lng` double(11,6),
        `Lat` double(11,6),
        `IsAlarmData` tinyint(1),
        `Source` tinyint(1),
        INDEX idx_device_time (DeviceAddr, RecordTime),
        INDEX idx_record_time (RecordTime)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    
    # Determine sync starting point
    if force_resync:
        # Clear existing data for full resync
        cursor.execute("DELETE FROM device_40377991_processed WHERE DeviceAddr = 40377991")
        start_time = datetime.now() - timedelta(days=days_back)
    else:
        # Get latest processed time for incremental sync
        cursor.execute("""
            SELECT MAX(RecordTime) as latest 
            FROM device_40377991_processed 
            WHERE DeviceAddr = 40377991
        """)
        result = cursor.fetchone()
        if result and result['latest']:
            start_time = result['latest']
        else:
            start_time = datetime.now() - timedelta(days=days_back)
    
    # Fetch raw data from tbhistory
    query = """
    SELECT DeviceName, DeviceAddr, NodeId, Hum, Tem,
           CoordinateType, Lng, Lat, RecordTime, IsAlarmData, Source
    FROM tbhistory
    WHERE DeviceAddr = 40377991 
    AND NodeId IN (1, 2, 3, 4, 5)
    AND RecordTime > %s
    ORDER BY RecordTime, NodeId
    """
    
    cursor.execute(query, (start_time,))
    raw_records = cursor.fetchall()
    
    if not raw_records:
        return {
            'records_processed': 0,
            'groups_created': 0,
            'message': 'No new records to sync'
        }
    
    # Group records by time window (60 seconds)
    time_groups = {}
    
    for record in raw_records:
        record_time = record['RecordTime']
        node_id = record['NodeId']
        
        # Find or create time group
        matched_key = None
        for key, group in time_groups.items():
            if abs((record_time - group['base_time']).total_seconds()) <= 60:
                matched_key = key
                break
        
        if matched_key:
            time_groups[matched_key]['nodes'][node_id] = record
            # Keep earliest timestamp
            if record_time < time_groups[matched_key]['base_time']:
                time_groups[matched_key]['base_time'] = record_time
                time_groups[matched_key]['base_record'] = record
        else:
            key = f"{record_time.strftime('%Y%m%d%H%M%S')}_{len(time_groups):06d}"
            time_groups[key] = {
                'base_time': record_time,
                'base_record': record,
                'nodes': {node_id: record}
            }
    
    # Process each time group
    processed_count = 0
    latest_time = None
    
    for group_key, group_data in time_groups.items():
        nodes = group_data['nodes']
        base_time = group_data['base_time']
        base_record = group_data['base_record']
        
        # Build processed record
        processed = {
            'ID': f"40377991_{base_time.strftime('%Y%m%d%H%M%S')}_{processed_count:06d}",
            'DeviceName': base_record.get('DeviceName', 'Comprehensive Vibration and Temperature Sensor'),
            'DeviceAddr': 40377991,
            'RecordTime': base_time,
            'CoordinateType': base_record['CoordinateType'],
            'Lng': base_record['Lng'],
            'Lat': base_record['Lat'],
            'Source': base_record['Source'],
            'IsAlarmData': 0
        }
        
        # Apply node mappings
        # Node 1: Hum=Temperature, Tem=X轴振动速度
        if 1 in nodes:
            processed['Temperature'] = nodes[1]['Hum']
            processed['X轴振动速度_mm_s'] = nodes[1]['Tem']
            processed['IsAlarmData'] = max(processed['IsAlarmData'], nodes[1].get('IsAlarmData', 0))
        
        # Node 2: Hum=Z轴振动速度, Tem=Y轴振动速度
        if 2 in nodes:
            processed['Z轴振动速度_mm_s'] = nodes[2]['Hum']
            processed['Y轴振动速度_mm_s'] = nodes[2]['Tem']
            processed['IsAlarmData'] = max(processed['IsAlarmData'], nodes[2].get('IsAlarmData', 0))
        
        # Node 3: Hum=Y_displacement, Tem=X_displacement
        if 3 in nodes:
            processed['Y_displacement_μm'] = nodes[3]['Hum']
            processed['X_displacement_μm'] = nodes[3]['Tem']
            processed['IsAlarmData'] = max(processed['IsAlarmData'], nodes[3].get('IsAlarmData', 0))
        
        # Node 4: Hum=X_acceleration, Tem=Z_displacement
        if 4 in nodes:
            processed['X_acceleration_m_s2'] = nodes[4]['Hum']
            processed['Z_displacement_μm'] = nodes[4]['Tem']
            processed['IsAlarmData'] = max(processed['IsAlarmData'], nodes[4].get('IsAlarmData', 0))
        
        # Node 5: Hum=Z_acceleration, Tem=Y_acceleration
        if 5 in nodes:
            processed['Z_acceleration_m_s2'] = nodes[5]['Hum']
            processed['Y_acceleration_m_s2'] = nodes[5]['Tem']
            processed['IsAlarmData'] = max(processed['IsAlarmData'], nodes[5].get('IsAlarmData', 0))
        
        # Set missing values to None
        for key in ['Temperature', 'X轴振动速度_mm_s', 'Y轴振动速度_mm_s', 'Z轴振动速度_mm_s',
                   'X_displacement_μm', 'Y_displacement_μm', 'X_acceleration_m_s2',
                   'Z_displacement_μm', 'Y_acceleration_m_s2', 'Z_acceleration_m_s2']:
            if key not in processed:
                processed[key] = None
        
        # Insert or update record
        insert_query = """
        INSERT INTO device_40377991_processed 
        (ID, DeviceName, DeviceAddr, Temperature, `X轴振动速度_mm_s`, `Y轴振动速度_mm_s`, 
         `Z轴振动速度_mm_s`, `X_displacement_μm`, `Y_displacement_μm`, X_acceleration_m_s2,
         `Z_displacement_μm`, Y_acceleration_m_s2, Z_acceleration_m_s2, RecordTime, 
         CoordinateType, Lng, Lat, IsAlarmData, Source)
        VALUES (%(ID)s, %(DeviceName)s, %(DeviceAddr)s, %(Temperature)s, %(X轴振动速度_mm_s)s,
                %(Y轴振动速度_mm_s)s, %(Z轴振动速度_mm_s)s, %(X_displacement_μm)s,
                %(Y_displacement_μm)s, %(X_acceleration_m_s2)s, %(Z_displacement_μm)s,
                %(Y_acceleration_m_s2)s, %(Z_acceleration_m_s2)s, %(RecordTime)s,
                %(CoordinateType)s, %(Lng)s, %(Lat)s, %(IsAlarmData)s, %(Source)s)
        ON DUPLICATE KEY UPDATE
            Temperature = VALUES(Temperature),
            `X轴振动速度_mm_s` = VALUES(`X轴振动速度_mm_s`),
            `Y轴振动速度_mm_s` = VALUES(`Y轴振动速度_mm_s`),
            `Z轴振动速度_mm_s` = VALUES(`Z轴振动速度_mm_s`),
            `X_displacement_μm` = VALUES(`X_displacement_μm`),
            `Y_displacement_μm` = VALUES(`Y_displacement_μm`),
            X_acceleration_m_s2 = VALUES(X_acceleration_m_s2),
            `Z_displacement_μm` = VALUES(`Z_displacement_μm`),
            Y_acceleration_m_s2 = VALUES(Y_acceleration_m_s2),
            Z_acceleration_m_s2 = VALUES(Z_acceleration_m_s2),
            IsAlarmData = VALUES(IsAlarmData)
        """
        
        cursor.execute(insert_query, processed)
        processed_count += 1
        
        if not latest_time or base_time > latest_time:
            latest_time = base_time
    
    connection.commit()
    
    return {
        'records_processed': len(raw_records),
        'groups_created': processed_count,
        'latest_record': latest_time.isoformat() if latest_time else None,
        'message': f'Successfully synced {processed_count} time groups from {len(raw_records)} raw records'
    }

def sync_device_40372539(cursor, connection, force_resync=False, days_back=30):
    """Sync Device 40372539 data with proper node mapping"""
    
    # Ensure table exists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS device_40372539_processed (
        `ID` varchar(128) PRIMARY KEY,
        `DeviceName` varchar(255) DEFAULT 'Temperature and XYZ Velocity Sensor',
        `DeviceAddr` int DEFAULT 40372539,
        `Temperature_C` double(20,4) COMMENT 'Node 7 Tem field',
        `X_velocity_mm_s` double(20,4) COMMENT 'Node 10 Tem field',
        `Y_velocity_mm_s` double(20,4) COMMENT 'Node 13 Tem field',
        `Z_velocity_mm_s` double(20,4) COMMENT 'Node 16 Tem field',
        `RecordTime` datetime,
        `CoordinateType` tinyint(1),
        `Lng` double(11,6),
        `Lat` double(11,6),
        `IsAlarmData` tinyint(1),
        `Source` tinyint(1),
        INDEX idx_device_time (DeviceAddr, RecordTime),
        INDEX idx_record_time (RecordTime)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    
    # Determine sync starting point
    if force_resync:
        cursor.execute("DELETE FROM device_40372539_processed WHERE DeviceAddr = 40372539")
        start_time = datetime.now() - timedelta(days=days_back)
    else:
        cursor.execute("""
            SELECT MAX(RecordTime) as latest 
            FROM device_40372539_processed 
            WHERE DeviceAddr = 40372539
        """)
        result = cursor.fetchone()
        if result and result['latest']:
            start_time = result['latest']
        else:
            start_time = datetime.now() - timedelta(days=days_back)
    
    # Fetch raw data
    query = """
    SELECT DeviceName, DeviceAddr, NodeId, Hum, Tem,
           CoordinateType, Lng, Lat, RecordTime, IsAlarmData, Source
    FROM tbhistory
    WHERE DeviceAddr = 40372539 
    AND NodeId IN (7, 10, 13, 16)
    AND RecordTime > %s
    ORDER BY RecordTime, NodeId
    """
    
    cursor.execute(query, (start_time,))
    raw_records = cursor.fetchall()
    
    if not raw_records:
        return {
            'records_processed': 0,
            'groups_created': 0,
            'message': 'No new records to sync'
        }
    
    # Group records by time window
    time_groups = {}
    
    for record in raw_records:
        record_time = record['RecordTime']
        node_id = record['NodeId']
        
        # Find or create time group
        matched_key = None
        for key, group in time_groups.items():
            if abs((record_time - group['base_time']).total_seconds()) <= 60:
                matched_key = key
                break
        
        if matched_key:
            time_groups[matched_key]['nodes'][node_id] = record
            if record_time < time_groups[matched_key]['base_time']:
                time_groups[matched_key]['base_time'] = record_time
                time_groups[matched_key]['base_record'] = record
        else:
            key = f"{record_time.strftime('%Y%m%d%H%M%S')}_{len(time_groups):06d}"
            time_groups[key] = {
                'base_time': record_time,
                'base_record': record,
                'nodes': {node_id: record}
            }
    
    # Process each time group
    processed_count = 0
    latest_time = None
    
    for group_key, group_data in time_groups.items():
        nodes = group_data['nodes']
        base_time = group_data['base_time']
        base_record = group_data['base_record']
        
        # Build processed record
        processed = {
            'ID': f"40372539_{base_time.strftime('%Y%m%d%H%M%S')}_{processed_count:06d}",
            'DeviceName': base_record.get('DeviceName', 'Temperature and XYZ Velocity Sensor'),
            'DeviceAddr': 40372539,
            'RecordTime': base_time,
            'CoordinateType': base_record['CoordinateType'],
            'Lng': base_record['Lng'],
            'Lat': base_record['Lat'],
            'Source': base_record['Source'],
            'IsAlarmData': 0
        }
        
        # Apply node mappings (only Tem field used for this device)
        # Node 7: Tem=Temperature
        if 7 in nodes:
            processed['Temperature_C'] = nodes[7]['Tem']
            processed['IsAlarmData'] = max(processed['IsAlarmData'], nodes[7].get('IsAlarmData', 0))
        
        # Node 10: Tem=X velocity
        if 10 in nodes:
            processed['X_velocity_mm_s'] = nodes[10]['Tem']
            processed['IsAlarmData'] = max(processed['IsAlarmData'], nodes[10].get('IsAlarmData', 0))
        
        # Node 13: Tem=Y velocity
        if 13 in nodes:
            processed['Y_velocity_mm_s'] = nodes[13]['Tem']
            processed['IsAlarmData'] = max(processed['IsAlarmData'], nodes[13].get('IsAlarmData', 0))
        
        # Node 16: Tem=Z velocity
        if 16 in nodes:
            processed['Z_velocity_mm_s'] = nodes[16]['Tem']
            processed['IsAlarmData'] = max(processed['IsAlarmData'], nodes[16].get('IsAlarmData', 0))
        
        # Set missing values to None
        for key in ['Temperature_C', 'X_velocity_mm_s', 'Y_velocity_mm_s', 'Z_velocity_mm_s']:
            if key not in processed:
                processed[key] = None
        
        # Insert or update record
        insert_query = """
        INSERT INTO device_40372539_processed 
        (ID, DeviceName, DeviceAddr, Temperature_C, X_velocity_mm_s, 
         Y_velocity_mm_s, Z_velocity_mm_s, RecordTime, CoordinateType, 
         Lng, Lat, IsAlarmData, Source)
        VALUES (%(ID)s, %(DeviceName)s, %(DeviceAddr)s, %(Temperature_C)s,
                %(X_velocity_mm_s)s, %(Y_velocity_mm_s)s, %(Z_velocity_mm_s)s,
                %(RecordTime)s, %(CoordinateType)s, %(Lng)s, %(Lat)s,
                %(IsAlarmData)s, %(Source)s)
        ON DUPLICATE KEY UPDATE
            Temperature_C = VALUES(Temperature_C),
            X_velocity_mm_s = VALUES(X_velocity_mm_s),
            Y_velocity_mm_s = VALUES(Y_velocity_mm_s),
            Z_velocity_mm_s = VALUES(Z_velocity_mm_s),
            IsAlarmData = VALUES(IsAlarmData)
        """
        
        cursor.execute(insert_query, processed)
        processed_count += 1
        
        if not latest_time or base_time > latest_time:
            latest_time = base_time
    
    connection.commit()
    
    return {
        'records_processed': len(raw_records),
        'groups_created': processed_count,
        'latest_record': latest_time.isoformat() if latest_time else None,
        'message': f'Successfully synced {processed_count} time groups from {len(raw_records)} raw records'
    }

# Batch sync endpoint for both devices
@app.route('/api/sync/all', methods=['POST'])
def sync_all_devices():
    """Sync data for all supported devices"""
    try:
        request_data = request.get_json() if request.is_json else {}
        force_resync = request_data.get('force_resync', False)
        days_back = request_data.get('days_back', 30)
        
        results = {
            'sync_timestamp': datetime.now().isoformat(),
            'devices': {}
        }
        
        # Sync both devices
        for device_addr in [40377991, 40372539]:
            sync_response = sync_device_data(device_addr)
            
            if sync_response.status_code == 200:
                results['devices'][device_addr] = sync_response.get_json()
            else:
                results['devices'][device_addr] = {
                    'error': 'Sync failed',
                    'status_code': sync_response.status_code
                }
        
        return jsonify(results)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Status check endpoint
@app.route('/api/sync/status', methods=['GET'])
def check_sync_status():
    """Check sync status and compare tbhistory vs processed tables"""
    try:
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = connection.cursor(dictionary=True)
        
        status = {
            'check_timestamp': datetime.now().isoformat(),
            'devices': {}
        }
        
        for device_addr in [40377991, 40372539]:
            # Get latest from tbhistory
            if device_addr == 40377991:
                node_filter = "AND NodeId IN (1, 2, 3, 4, 5)"
                table_name = "device_40377991_processed"
            else:
                node_filter = "AND NodeId IN (7, 10, 13, 16)"
                table_name = "device_40372539_processed"
            
            cursor.execute(f"""
                SELECT MAX(RecordTime) as latest_raw, COUNT(*) as raw_count
                FROM tbhistory
                WHERE DeviceAddr = %s {node_filter}
            """, (device_addr,))
            raw_info = cursor.fetchone()
            
            # Get latest from processed table
            cursor.execute(f"""
                SELECT MAX(RecordTime) as latest_processed, COUNT(*) as processed_count
                FROM {table_name}
                WHERE DeviceAddr = %s
            """, (device_addr,))
            processed_info = cursor.fetchone()
            
            # Calculate sync status
            needs_sync = False
            if raw_info['latest_raw'] and processed_info['latest_processed']:
                needs_sync = raw_info['latest_raw'] > processed_info['latest_processed']
            elif raw_info['latest_raw'] and not processed_info['latest_processed']:
                needs_sync = True
            
            status['devices'][device_addr] = {
                'tbhistory': {
                    'latest_record': raw_info['latest_raw'].isoformat() if raw_info['latest_raw'] else None,
                    'total_records': raw_info['raw_count']
                },
                'processed_table': {
                    'table_name': table_name,
                    'latest_record': processed_info['latest_processed'].isoformat() if processed_info['latest_processed'] else None,
                    'total_records': processed_info['processed_count']
                },
                'needs_sync': needs_sync,
                'records_behind': raw_info['raw_count'] - (processed_info['processed_count'] * 5) if processed_info['processed_count'] else raw_info['raw_count']
            }
        
        cursor.close()
        connection.close()
        
        return jsonify(status)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500 
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
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)