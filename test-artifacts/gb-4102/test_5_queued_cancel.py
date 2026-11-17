"""Scenario 5: cancelling a queued experiment means it never executes.

The queued experiment transitions straight to cancelled without touching the
database; when the worker later pops its queue entry, the claim fails and the
experiment is skipped.
"""
import time

from common import (check, summary, login, create_exp, cancel, db,
                    wait_for, wait_status, get_exp, unique_name)

TERMINAL = ('completed', 'failed', 'cancelled', 'timeout')

def main():
    token = login()
    rogue = db()
    rogue.cursor().execute("SELECT GET_LOCK('index_trial_serial', 0)")
    try:
        blocker = create_exp(token, unique_name('queue-block'), limit=60)
        wait_status(token, blocker, ('running',), timeout=15)  # worker busy on the lock
        eid = create_exp(token, unique_name('queue-cancel'), limit=60)
        time.sleep(0.5)
        before = get_exp(token, eid)
        check('queued-cancel: still queued behind busy worker', before['status'] == 'queued', before['status'])
        code, body = cancel(token, eid)
        check('queued-cancel: immediate cancelled terminal', code == 200 and body['status'] == 'cancelled',
              '%s %s' % (code, body))
        check('queued-cancel: never started', body['started_at'] is None and body['result'] is None)
        code2, _ = cancel(token, eid)
        check('queued-cancel: repeat cancel rejected (already terminal)', code2 == 409, code2)
    finally:
        rogue.cursor().execute("SELECT RELEASE_LOCK('index_trial_serial')")
        rogue.close()
    done = wait_status(token, blocker, TERMINAL, timeout=60)
    check('queued-cancel: blocker experiment completes after unlock', done is not None and done['status'] == 'completed',
          done and done['status'])
    time.sleep(4)  # worker pops the stale queue entry here
    after = get_exp(token, eid)
    check('queued-cancel: never executed after queue drain',
          after['status'] == 'cancelled' and after['started_at'] is None and after['result'] is None,
          '%s started=%s' % (after['status'], after['started_at']))
    return summary()

if __name__ == '__main__':
    raise SystemExit(main())
