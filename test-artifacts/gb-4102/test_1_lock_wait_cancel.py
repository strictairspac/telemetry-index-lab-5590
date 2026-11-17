"""Scenario 1: cancel while the trial waits on a database lock.

Three lock shapes. In all of them the DB execution must stop within 10 seconds
of the cancel request (the running statement is KILLed). The terminal state
appears once cleanup is confirmed (probe rollback, temp index gone):
  A. named lock  - GET_LOCK holder; telemetry stays readable -> cancelled while locked
  B. write lock  - LOCK TABLES telemetry WRITE; cleanup cannot be confirmed while
                   the lock is held -> must stay cancelling/cleanup_pending (never a
                   false terminal), then reach cancelled soon after UNLOCK
  C. read lock   - LOCK TABLES telemetry READ; reads still possible -> cancelled while locked
"""
import time

from common import (check, summary, login, create_exp, cancel, db, probe_db,
                    wait_for, wait_status, get_exp, index_exists, conn_active,
                    conn_activity, unique_name)

def run_case(token, tag, lock_sql, unlock_sql, wait_phase, terminal_while_locked):
    rogue = db()
    cur = rogue.cursor()
    cur.execute(lock_sql)
    eid = create_exp(token, unique_name(tag), limit=120)
    try:
        stuck = wait_status(token, eid, ('running',), timeout=15)
        check(tag + ': trial started while lock held', stuck is not None)
        got = wait_for(lambda: get_exp(token, eid)['phase'] == wait_phase and get_exp(token, eid) or None, timeout=15)
        check(tag + ': trial blocked in phase ' + wait_phase, got is not None,
              'phase=' + str(get_exp(token, eid)['phase']))
        conn_id = get_exp(token, eid).get('connection_id')
        t0 = time.time()
        code, body = cancel(token, eid)
        check(tag + ': cancel accepted (200, cancelling)', code == 200 and body['status'] == 'cancelling',
              '%s %s' % (code, body))
        # hard requirement: DB execution stops within 10s even while the lock is held
        stopped = None
        deadline = time.time() + 10
        probe = probe_db()
        while time.time() < deadline:
            if conn_id and conn_active(probe.cursor(), conn_id) == 0:
                stopped = time.time() - t0
                break
            time.sleep(0.2)
        probe.close()
        check(tag + ': db execution stopped within 10s of cancel', stopped is not None,
              'still active after 10s')
        if terminal_while_locked:
            final = wait_status(token, eid, ('cancelled',), timeout=10)
            check(tag + ': cancelled within 10s of cancel', final is not None,
                  'status=%s' % get_exp(token, eid)['status'])
        else:
            time.sleep(3)
            mid = get_exp(token, eid)
            check(tag + ': no false terminal while cleanup unconfirmable',
                  mid['status'] in ('cancelling', 'cleanup_pending'), mid['status'])
        cur.execute(unlock_sql)
        final = wait_status(token, eid, ('cancelled',), timeout=20)
        check(tag + ': cancelled after cleanup confirmable', final is not None,
              'status=%s' % get_exp(token, eid)['status'])
        check(tag + ': reason recorded', final is not None and final['reason'] and '取消' in final['reason'],
              final and final['reason'])
        c2 = probe_db().cursor()
        check(tag + ': temp index cleaned', not index_exists(c2))
        if conn_id:
            active, leaked = conn_activity(c2, conn_id)
            check(tag + ': probe rolled back, no leaked rows', leaked == 0,
                  'leaked_probe_rows=%s' % leaked)
    finally:
        try:
            cur.execute(unlock_sql)
        except Exception:
            pass
        rogue.close()

def main():
    token = login()
    run_case(token, 'A-named-lock',
             "SELECT GET_LOCK('index_trial_serial', 0)",
             "SELECT RELEASE_LOCK('index_trial_serial')", 'locking', True)
    run_case(token, 'B-write-lock', 'LOCK TABLES telemetry WRITE', 'UNLOCK TABLES', 'baseline', False)
    run_case(token, 'C-read-lock', 'LOCK TABLES telemetry READ', 'UNLOCK TABLES', 'baseline', True)
    return summary()

if __name__ == '__main__':
    raise SystemExit(main())
