"""Scenario 3: executor process interruption and cleanup recovery.

  1. Real SIGKILL mid-trial: orphan experiment is finalized (failed/中断),
     leftover index dropped, later experiments continue.
  2. Interrupted cleanup (cleanup_pending + leftover index while worker is
     down): startup recovery finishes the cancel -> cancelled.
  3. Index ownership: force_cleanup never drops the trial index while another
     executor holds the serial lock; it cleans once ownership is attributable.
"""
import os, sys, time

BACKEND = os.environ.get('BACKEND_DIR', '/workspace/backend')
sys.path.insert(0, BACKEND)
os.environ.setdefault('DATABASE_URL', 'mysql+pymysql://trial:trialpass@127.0.0.1:17741/indextrial')
os.environ.setdefault('REDIS_URL', 'redis://127.0.0.1:6379/0')
os.environ.setdefault('JWT_SECRET', 'local-test-secret')

from common import (check, summary, login, create_exp, cancel, db, probe_db, run_cmd,
                    wait_for, wait_status, get_exp, index_exists, unique_name)
from src.services.index_algorithm import force_cleanup

KILL_CMD = os.environ.get('WORKER_KILL_CMD', 'docker compose kill -s SIGKILL worker')
START_CMD = os.environ.get('WORKER_START_CMD', 'docker compose up -d worker')
TERMINAL = ('completed', 'failed', 'cancelled', 'timeout')

def wait_worker_ready(token, timeout=60):
    """Worker is up when a fresh experiment gets claimed."""
    eid = create_exp(token, unique_name('recover-probe'), limit=60)
    return wait_status(token, eid, ('running',) + TERMINAL, timeout=timeout) is not None

def main():
    token = login()

    # --- part 1: SIGKILL the worker mid-trial ---------------------------------
    rogue = db()
    rogue.cursor().execute("SELECT GET_LOCK('index_trial_serial', 0)")
    eid = create_exp(token, unique_name('crash'), limit=120)
    wait_status(token, eid, ('running',), timeout=15)  # blocked in 'locking' phase
    run_cmd(KILL_CMD)
    time.sleep(1)
    mid = get_exp(token, eid)
    check('crash: experiment left running after SIGKILL', mid['status'] == 'running', mid['status'])
    rogue.cursor().execute("SELECT RELEASE_LOCK('index_trial_serial')")
    rogue.close()
    run_cmd(START_CMD)
    final = wait_status(token, eid, TERMINAL, timeout=40)
    check('crash: orphan reaches terminal state after restart', final is not None,
          get_exp(token, eid)['status'])
    check('crash: interrupted trial marked failed',
          final is not None and final['status'] == 'failed', final and final['status'])
    if final and final['status'] == 'failed':
        check('crash: failure reason mentions interruption',
              bool(final['error']) and '中断' in final['error'], final['error'])
    c = probe_db().cursor()
    check('crash: no leftover trial index', not index_exists(c))
    e2 = create_exp(token, unique_name('crash-next'), limit=60)
    nxt = wait_status(token, e2, TERMINAL, timeout=60)
    check('crash: subsequent experiment completes', nxt is not None and nxt['status'] == 'completed',
          nxt and nxt['status'])

    # --- part 2: cleanup interrupted (cleanup_pending) ------------------------
    run_cmd(KILL_CMD)
    time.sleep(1)
    e3 = create_exp(token, unique_name('crash-clean'), limit=120)
    conn = db()
    cur = conn.cursor()
    cur.execute("UPDATE experiments SET status='cleanup_pending', stop_kind='user', "
                "reason='清理待处理：模拟中断', phase='cleanup' WHERE id=%s AND status='queued'", (e3,))
    assert cur.rowcount == 1
    cur.execute('CREATE INDEX trial_candidate ON telemetry (sensor_id)')
    conn.close()
    run_cmd(START_CMD)
    final = wait_status(token, e3, ('cancelled',), timeout=40)
    check('cleanup-interrupt: startup recovery finishes cancel', final is not None,
          get_exp(token, e3)['status'])
    c = probe_db().cursor()
    check('cleanup-interrupt: leftover index dropped', not index_exists(c))

    # --- part 3: never drop another trial's index ------------------------------
    rogue = db()
    rogue.cursor().execute("SELECT GET_LOCK('index_trial_serial', 0)")
    rogue.cursor().execute('CREATE INDEX trial_candidate ON telemetry (sensor_id)')
    ok, msg = force_cleanup()
    check('ownership: cleanup refused while serial lock held by others', ok is False, msg)
    c = probe_db().cursor()
    check('ownership: foreign trial index untouched', index_exists(c))
    rogue.cursor().execute("SELECT RELEASE_LOCK('index_trial_serial')")
    rogue.close()
    ok, msg = force_cleanup()
    check('ownership: cleanup proceeds once lock attributable', ok is True, msg)
    c = probe_db().cursor()
    check('ownership: orphan index cleaned', not index_exists(c))
    return summary()

if __name__ == '__main__':
    raise SystemExit(main())
