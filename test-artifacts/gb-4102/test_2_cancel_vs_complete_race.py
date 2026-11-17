"""Scenario 2: cancel racing completion keeps exactly one terminal state.

A late success from the executor must never overwrite a cancel, and a cancel
arriving after completion must be rejected (409). Sweeps cancel delays around
the measured trial duration to hit the race window from both sides.
"""
import time

from common import (check, summary, login, create_exp, cancel, probe_db,
                    wait_for, wait_status, get_exp, index_exists, unique_name)

TERMINAL = ('completed', 'failed', 'cancelled', 'timeout')

def measure_duration(token):
    eid = create_exp(token, unique_name('race-calib'), limit=120)
    t0 = time.time()
    final = wait_status(token, eid, TERMINAL, timeout=60)
    assert final and final['status'] == 'completed', final
    return time.time() - t0

def main():
    token = login()
    duration = measure_duration(token)
    print('measured trial duration: %.2fs' % duration)
    ok = True
    for frac in (0.4, 0.75, 0.9, 1.0, 1.15, 1.5):
        eid = create_exp(token, unique_name('race'), limit=120)
        time.sleep(max(0.05, duration * frac - 0.3))
        code, body = cancel(token, eid)
        accepted = code == 200
        final = wait_status(token, eid, TERMINAL, timeout=30)
        tag = 'race@%.2f' % frac
        check(tag + ': reaches exactly one terminal state', final is not None and final['status'] in TERMINAL,
              final and final['status'])
        if not final:
            ok = False
            continue
        if accepted and body['status'] == 'cancelling':
            check(tag + ': accepted cancel is never overwritten by late success',
                  final['status'] == 'cancelled', final['status'])
        elif code == 409:
            check(tag + ': rejected cancel means completion won', final['status'] == 'completed', final['status'])
        # terminal state is stable: observe a little longer
        time.sleep(2)
        after = get_exp(token, eid)
        check(tag + ': terminal state stable', after['status'] == final['status'],
              '%s -> %s' % (final['status'], after['status']))
        c = probe_db().cursor()
        check(tag + ': no leftover trial index', not index_exists(c))
    # cancel arriving strictly after completion must be rejected and ignored
    eid = create_exp(token, unique_name('race-late'), limit=120)
    final = wait_status(token, eid, TERMINAL, timeout=60)
    assert final and final['status'] == 'completed', final
    code, body = cancel(token, eid)
    check('late-cancel: rejected with 409', code == 409, '%s %s' % (code, body))
    time.sleep(1)
    check('late-cancel: completed state untouched', get_exp(token, eid)['status'] == 'completed')
    return summary()

if __name__ == '__main__':
    raise SystemExit(main())
