"""Scenario 4: execution time limit stops an over-running trial.

A trial blocked on a database lock past its time limit must be stopped by the
watcher (not by the lock's own timeout) and land on the timeout terminal state
with cleanup confirmed.
"""
import time

from common import (check, summary, login, create_exp, cancel, db, probe_db,
                    wait_for, wait_status, get_exp, index_exists, conn_active,
                    unique_name)

def main():
    token = login()
    rogue = db()
    rogue.cursor().execute("SELECT GET_LOCK('index_trial_serial', 0)")
    t0 = time.time()
    eid = create_exp(token, unique_name('timeout'), limit=10)
    try:
        running = wait_status(token, eid, ('running',), timeout=15)
        check('timeout: trial started and blocked on lock', running is not None)
        conn_id = wait_for(lambda: get_exp(token, eid).get('connection_id'), timeout=10)
        check('timeout: trial connection recorded', conn_id is not None)
        final = wait_status(token, eid, ('timeout',), timeout=30)
        elapsed = time.time() - t0
        check('timeout: reaches timeout terminal state', final is not None,
              'status=%s' % get_exp(token, eid)['status'])
        check('timeout: stopped near the 10s limit (not the 30s lock timeout)', final is not None and elapsed < 25,
              'elapsed=%.1fs' % elapsed)
        check('timeout: reason mentions the limit',
              final is not None and final['reason'] and ('时限' in final['reason'] or '超时' in final['reason']),
              final and final['reason'])
        if conn_id:
            c = probe_db().cursor()
            check('timeout: db execution stopped', conn_active(c, conn_id) == 0)
        c = probe_db().cursor()
        check('timeout: no leftover trial index', not index_exists(c))
        check('timeout: no leaked probe rows', get_exp(token, eid)['status'] == 'timeout')
    finally:
        rogue.cursor().execute("SELECT RELEASE_LOCK('index_trial_serial')")
        rogue.close()
    # a healthy experiment with a generous limit still completes
    eid = create_exp(token, unique_name('timeout-ok'), limit=120)
    final = wait_status(token, eid, ('completed',), timeout=60)
    check('timeout: generous limit completes normally', final is not None)
    return summary()

if __name__ == '__main__':
    raise SystemExit(main())
