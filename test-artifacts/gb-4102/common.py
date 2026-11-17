"""Shared helpers for the gb-4102 verification suite.

Targets the local compose stack by default; override with env:
  BASE_URL, DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME,
  WORKER_KILL_CMD, WORKER_START_CMD  (crash-recovery test only)
"""
import json, os, subprocess, sys, time, urllib.request, urllib.error

import pymysql

BASE = os.environ.get('BASE_URL', 'http://127.0.0.1:19241')
DB = dict(host=os.environ.get('DB_HOST', '127.0.0.1'),
          port=int(os.environ.get('DB_PORT', '17741')),
          user=os.environ.get('DB_USER', 'trial'),
          password=os.environ.get('DB_PASS', 'trialpass'),
          database=os.environ.get('DB_NAME', 'indextrial'))
TRIAL_INDEX = 'trial_candidate'

PASS, FAIL = [], []

def check(name, cond, detail=''):
    (PASS if cond else FAIL).append(name)
    print(('PASS ' if cond else 'FAIL ') + name + (('  -- ' + str(detail)) if detail and not cond else ''))

def summary():
    print(f'\n{len(PASS)} passed, {len(FAIL)} failed')
    if FAIL:
        print('failed: ' + ', '.join(FAIL))
    return 1 if FAIL else 0

def login():
    with urllib.request.urlopen(urllib.request.Request(
            BASE + '/auth/login', method='POST',
            data=json.dumps({'email': 'operator@index.local', 'password': 'Trial2026!'}).encode(),
            headers={'Content-Type': 'application/json'})) as r:
        return json.load(r)['token']

def api(path, token, method='GET', body=None):
    req = urllib.request.Request(BASE + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={'Content-Type': 'application/json',
                                          'Authorization': 'Bearer ' + token})
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, json.load(e)

def db():
    return pymysql.connect(**DB, autocommit=True)

def probe_db():
    """Inspection connection whose metadata-lock waits are bounded."""
    conn = db()
    cur = conn.cursor()
    cur.execute('SET SESSION lock_wait_timeout=2')
    return conn

def conn_active(cur, conn_id):
    cur.execute("SELECT COUNT(*) FROM information_schema.processlist "
                "WHERE ID=%s AND COMMAND NOT IN ('Sleep','Daemon','Connect')", (conn_id,))
    return cur.fetchone()[0]

def get_exp(token, eid):
    return api('/experiments/%d' % eid, token)[1]

def wait_for(fn, timeout=30, interval=0.25):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = fn()
        if value:
            return value
        time.sleep(interval)
    return None

def wait_status(token, eid, targets, timeout=30):
    return wait_for(lambda: get_exp(token, eid) if get_exp(token, eid)['status'] in targets else None, timeout)

def create_exp(token, name, limit=120, candidate_id=1):
    code, body = api('/experiments', token, 'POST',
                     {'name': name, 'candidate_id': candidate_id, 'time_limit_seconds': limit})
    assert code == 200, body
    return body['id']

def cancel(token, eid):
    return api('/experiments/%d/cancel' % eid, token, 'POST', {})

def index_exists(cur):
    cur.execute("SELECT COUNT(*) FROM information_schema.statistics "
                "WHERE table_schema=DATABASE() AND table_name='telemetry' AND index_name=%s", (TRIAL_INDEX,))
    return cur.fetchone()[0] > 0

def conn_activity(cur, conn_id):
    return conn_active(cur, conn_id), leaked_probe_rows(cur)

def leaked_probe_rows(cur):
    cur.execute('SELECT COUNT(*) FROM telemetry WHERE sensor_id=999')
    return cur.fetchone()[0]

def run_cmd(cmd):
    subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)

def unique_name(tag):
    return 'gb4102-%s-%d' % (tag, int(time.time() * 1000) % 10**9)
