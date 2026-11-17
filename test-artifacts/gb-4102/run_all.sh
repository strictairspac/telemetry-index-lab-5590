#!/bin/sh
# gb-4102 verification suite: cancel + execution time limit for index trials.
# Prereqs: stack reachable (compose up, or local equivalents). Env overrides:
#   BASE_URL DB_HOST DB_PORT DB_USER DB_PASS DB_NAME WORKER_KILL_CMD WORKER_START_CMD
set -e
cd "$(dirname "$0")"
rc=0
for t in test_1_lock_wait_cancel.py test_2_cancel_vs_complete_race.py test_2b_late_success.py test_3_crash_recovery.py test_4_timeout.py test_5_queued_cancel.py; do
  echo "================ $t"
  python3 "$t" || rc=1
done
echo "================ done (rc=$rc)"
exit $rc
