"""Scenario 2b (mechanism level): a late success from an old executor must not
overwrite a cancel. Drives the app's own lifecycle.complete() against
experiments that already reached a cancelled terminal state.
"""
import os, sys, time

BACKEND = os.environ.get('BACKEND_DIR', '/workspace/backend')
sys.path.insert(0, BACKEND)
os.environ.setdefault('DATABASE_URL', 'mysql+pymysql://trial:trialpass@127.0.0.1:17741/indextrial')
os.environ.setdefault('REDIS_URL', 'redis://127.0.0.1:6379/0')
os.environ.setdefault('JWT_SECRET', 'local-test-secret')

from common import (check, summary, login, create_exp, cancel, db, wait_status,
                    wait_for, get_exp, unique_name)
from src.services import lifecycle

TERMINAL = ('completed', 'failed', 'cancelled', 'timeout')

def main():
    token = login()
    # experiment cancelled while queued -> terminal cancelled; worker's late complete() must lose
    rogue = db()
    rogue.cursor().execute("SELECT GET_LOCK('index_trial_serial', 0)")
    blocker = create_exp(token, unique_name('late-block'), limit=120)
    wait_status(token, blocker, ('running',), timeout=15)  # worker busy on the lock
    eid = create_exp(token, unique_name('late-q'), limit=120)
    code, body = cancel(token, eid)
    rogue.cursor().execute("SELECT RELEASE_LOCK('index_trial_serial')")
    rogue.close()
    assert code == 200 and body['status'] == 'cancelled', body
    won = lifecycle.complete(eid, {'forged': True})
    time.sleep(0.5)
    after = get_exp(token, eid)
    check('late success on queued-cancel: complete() loses', won is False)
    check('late success on queued-cancel: still cancelled, no result',
          after['status'] == 'cancelled' and not after['result'],
          '%s result=%s' % (after['status'], bool(after['result'])))
    # experiment cancelled while running -> cancelled; worker's late complete() must lose
    rogue = db()
    rogue.cursor().execute("SELECT GET_LOCK('index_trial_serial', 0)")
    eid = create_exp(token, unique_name('late-r'), limit=120)
    wait_status(token, eid, ('running',), timeout=15)
    code, body = cancel(token, eid)
    rogue.cursor().execute("SELECT RELEASE_LOCK('index_trial_serial')")
    rogue.close()
    assert code == 200, body
    final = wait_status(token, eid, ('cancelled',), timeout=15)
    assert final, get_exp(token, eid)
    won = lifecycle.complete(eid, {'forged': True})
    time.sleep(0.5)
    after = get_exp(token, eid)
    check('late success on running-cancel: complete() loses', won is False)
    check('late success on running-cancel: still cancelled, no result',
          after['status'] == 'cancelled' and not after['result'],
          '%s result=%s' % (after['status'], bool(after['result'])))
    # sanity: complete() does win for a genuinely running, uncancelled experiment
    eid = create_exp(token, unique_name('late-ok'), limit=120)
    final = wait_status(token, eid, TERMINAL, timeout=60)
    check('healthy experiment completes normally', final is not None and final['status'] == 'completed',
          final and final['status'])
    return summary()

if __name__ == '__main__':
    raise SystemExit(main())
