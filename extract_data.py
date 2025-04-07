import sqlite3
import json
from datetime import datetime

TIME_FORMAT = '%Y%m%d-%H%M%S'

# Adjust these paths to your environment
CEPHVFS_PATH = "/usr/lib/libcephsqlite.so"  # Path to the Ceph VFS shared library
DB_PATH = "///.mgr:devicehealth/main.db"                     # Path to your SQLite database file

# Step 1: Open a dummy connection to load the extension
bootstrap_conn = sqlite3.connect(":memory:")
bootstrap_conn.enable_load_extension(True)

try:
    bootstrap_conn.load_extension(CEPHVFS_PATH)
    print("Ceph VFS extension loaded successfully.")
except sqlite3.OperationalError as e:
    print("Failed to load Ceph VFS:", e)
    bootstrap_conn.close()
    exit(1)

bootstrap_conn.close()

# Connect to SQLite with Ceph VFS (URI mode needed for VFS selection)
conn = sqlite3.connect(f"file:{DB_PATH}?vfs=ceph", uri=True)
conn.row_factory = sqlite3.Row

SQL_MIN = """
SELECT time, raw_smart
    FROM DeviceHealthMetrics
    WHERE devid = ? AND ? <= time
    ORDER BY time DESC;
"""

res = {}

# Query database and print tables
cur = conn.cursor()
try:
    cur.execute(SQL_MIN, ("ST5000LM000-2U8170_WCJ4Y8F7", 0))
    rows = cur.fetchall()
    for row in rows:
        t = row['time']
        dt = datetime.utcfromtimestamp(t).strftime(TIME_FORMAT)
        try:
            res[dt] = json.loads(row['raw_smart'])
        except (ValueError, IndexError):
            self.log.debug(f"unable to parse value for {devid}:{t}")
            pass
finally:
    conn.close()

health_data = res

predict_datas = []

if len(health_data) >= 6:
    o_keys = sorted(health_data.keys(), reverse=True)
    for o_key in o_keys:
        # get values for current day (?)
        dev_smart = {}
        s_val = health_data[o_key]

        # add all smart attributes
        ata_smart = s_val.get('ata_smart_attributes', {})
        for attr in ata_smart.get('table', []):
            # get raw smart values
            if attr.get('raw', {}).get('string') is not None:
                if str(attr.get('raw', {}).get('string', '0')).isdigit():
                    dev_smart['smart_%s_raw' % attr.get('id')] = \
                        int(attr.get('raw', {}).get('string', '0'))
                else:
                    if str(attr.get('raw', {}).get('string', '0')).split(' ')[0].isdigit():
                        dev_smart['smart_%s_raw' % attr.get('id')] = \
                            int(attr.get('raw', {}).get('string', '0').split(' ')[0])
                    else:
                        dev_smart['smart_%s_raw' % attr.get('id')] = \
                            attr.get('raw', {}).get('value', 0)
            # get normalized smart values
            if attr.get('value') is not None:
                dev_smart['smart_%s_normalized' % attr.get('id')] = \
                    attr.get('value')
        # add power on hours manually if not available in smart attributes
        power_on_time = s_val.get('power_on_time', {}).get('hours')
        if power_on_time is not None:
            dev_smart['smart_9_raw'] = int(power_on_time)
        # add device capacity
        user_capacity = s_val.get('user_capacity', {}).get('bytes')
        if user_capacity is not None:
            dev_smart['user_capacity'] = user_capacity
        else:
            print('user_capacity not found in smart attributes list')
        # add device model
        model_name = s_val.get('model_name')
        if model_name is not None:
            dev_smart['model_name'] = model_name
        # add vendor
        vendor = s_val.get('vendor')
        if vendor is not None:
            dev_smart['vendor'] = vendor
        # if smart data was found, then add that to list
        if dev_smart:
            predict_datas.append(dev_smart)
        if len(predict_datas) >= 12:
            break
else:
    print('unable to predict device due to health data records less than 6 days')

with open('predict_datas.json', 'w') as f:
    json.dump(predict_datas, f, indent=4) 
