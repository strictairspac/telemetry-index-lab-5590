"""Experiment execution lifecycle: atomic status transitions, cancel/timeout
watching, cleanup verification and interrupted-trial recovery.

Status machine (every transition is a conditional UPDATE, so concurrent actors
keep exactly one winner):
  queued   --cancel--> cancelled            (never touches the database)
  queued   --claim---> running
  running  --cancel--> cancelling --cleanup ok--> cancelled
  running  --timeout-> cancelling --cleanup ok--> timeout
  running  --success-> completed            (a late success never overwrites a cancel)
  running  --error---> failed
  cancelling/cleanup failure --> cleanup_pending --janitor--> terminal
  worker crash: running/cancelling orphans are recovered at startup.
"""
import logging,threading,time
from pathlib import Path
from sqlalchemy import text,update,func
from src.config.database import Session,engine
from src.models.experiment import Experiment
from src.services.index_algorithm import TrialInterrupted,verify_clean,force_cleanup
from src.services.audit import log
POLL_INTERVAL=0.5
class TrialControl:
 """Shared state between the trial thread and its watcher."""
 def __init__(self,run_id):
  self.run_id=run_id
  self.stop_event=threading.Event()
  self.cleanup_started=threading.Event()
  self.done=threading.Event()
  self.connection_id=None
 def should_stop(self):return self.stop_event.is_set()
 def set_connection_id(self,cid):
  self.connection_id=cid
  try:
   with Session() as s:s.execute(update(Experiment).where(Experiment.id==self.run_id).values(connection_id=cid));s.commit()
  except Exception:logging.exception('record connection id failed')
 def set_phase(self,phase):
  try:
   with Session() as s:s.execute(update(Experiment).where(Experiment.id==self.run_id,Experiment.status.in_(('running','cancelling'))).values(phase=phase));s.commit()
  except Exception:logging.exception('record phase failed')
def claim(run_id):
 """queued -> running; fails when the experiment was cancelled while queued."""
 with Session() as s:
  n=s.execute(update(Experiment).where(Experiment.id==run_id,Experiment.status=='queued').values(status='running',phase='locking',started_at=func.now())).rowcount
  s.commit();return n==1
def _kill_query(connection_id):
 try:
  with engine.connect() as c:c.execute(text(f'KILL QUERY {int(connection_id)}'))
 except Exception:logging.debug('kill query failed',exc_info=True)
def watch(control):
 """Detect cancel requests and deadline overruns, then interrupt the DB execution.
 KILL QUERY stops lock waits (GET_LOCK/MDL/row locks) within a fraction of a second."""
 while not control.done.wait(POLL_INTERVAL):
  Path('/tmp/worker-heartbeat').touch()
  try:
   with Session() as s:
    row=s.execute(text('SELECT status,TIME_TO_SEC(TIMEDIFF(NOW(),started_at)) AS elapsed,time_limit_seconds FROM experiments WHERE id=:i'),{'i':control.run_id}).mappings().first()
   if not row or row['status'] not in ('running','cancelling'):return
   stop=row['status']=='cancelling'
   if not stop and row['elapsed'] is not None and row['elapsed']>=row['time_limit_seconds']:
    with Session() as s:
     n=s.execute(update(Experiment).where(Experiment.id==control.run_id,Experiment.status=='running').values(status='cancelling',stop_kind='timeout',reason=f"执行超过 {row['time_limit_seconds']} 秒时限，正在停止数据库执行")).rowcount
     s.commit()
    stop=n==1
    if stop:log('worker','experiment.timeout',str(control.run_id))
   if stop:
    control.stop_event.set()
    if not control.cleanup_started.is_set() and control.connection_id:_kill_query(control.connection_id)
  except Exception:logging.exception('watcher iteration failed');time.sleep(1)
def complete(run_id,result):
 """running -> completed atomically. Returns False when a cancel won the race;
 the late result must be discarded instead of overwriting the cancel."""
 with Session() as s:
  n=s.execute(update(Experiment).where(Experiment.id==run_id,Experiment.status=='running').values(status='completed',phase='done',result=result,finished_at=func.now())).rowcount
  s.commit()
 if n:log('worker','experiment.completed',str(run_id))
 return n==1
def _target_for(stop_kind):return 'timeout' if stop_kind=='timeout' else 'cancelled'
def _brief(details):
 parts=[f'{k}={v}' for k,v in details.items() if v]
 return ('；'.join(parts) or '清理未确认')[:300]
def cleanup_and_verify(connection_id):
 ok,details=verify_clean(connection_id)
 if ok:return ok,details
 try:force_cleanup()
 except Exception:logging.exception('force cleanup failed')
 return verify_clean(connection_id)
def finalize_stop(run_id,control):
 """Cancel/timeout: publish the terminal state only after cleanup is confirmed."""
 with Session() as s:row=s.execute(text('SELECT stop_kind FROM experiments WHERE id=:i'),{'i':run_id}).mappings().first()
 if not row:return
 target=_target_for(row['stop_kind'])
 ok,details=cleanup_and_verify(control.connection_id)
 reason='执行超时，数据库执行已停止，临时对象已清理' if target=='timeout' else '用户取消，数据库执行已停止，临时对象已清理'
 with Session() as s:
  if ok:
   n=s.execute(update(Experiment).where(Experiment.id==run_id,Experiment.status=='cancelling').values(status=target,phase='done',reason=reason,finished_at=func.now())).rowcount
  else:
   n=s.execute(update(Experiment).where(Experiment.id==run_id,Experiment.status=='cancelling').values(status='cleanup_pending',reason=reason[:150]+'；清理待处理：'+_brief(details))).rowcount
  s.commit()
 if n:log('worker','experiment.'+(target if ok else 'cleanup_pending'),str(run_id))
def finalize_failure(run_id,control,exc):
 with Session() as s:current=s.execute(text('SELECT status FROM experiments WHERE id=:i'),{'i':run_id}).scalar()
 if current=='cancelling':finalize_stop(run_id,control);return
 ok,details=cleanup_and_verify(control.connection_id)
 error=str(exc)[:1000]
 with Session() as s:
  if ok:n=s.execute(update(Experiment).where(Experiment.id==run_id,Experiment.status=='running').values(status='failed',phase='done',error=error,finished_at=func.now())).rowcount
  else:n=s.execute(update(Experiment).where(Experiment.id==run_id,Experiment.status=='running').values(status='cleanup_pending',error=error,reason='清理待处理：'+_brief(details))).rowcount
  s.commit()
 if n:
  log('worker','experiment.failed',str(run_id));return
 with Session() as s:now=s.execute(text('SELECT status FROM experiments WHERE id=:i'),{'i':run_id}).scalar()
 if now=='cancelling':finalize_stop(run_id,control)
def _finalize_orphan(row):
 """Finish one ownerless active experiment (crashed executor or failed finalize).
 Only runs when no trial is executing in this process."""
 ok,details=cleanup_and_verify(row['connection_id'])
 if row['status']=='cancelling':target=_target_for(row['stop_kind'])
 elif row['status']=='cleanup_pending':target=_target_for(row['stop_kind']) if row['stop_kind'] else 'failed'
 else:target='failed'
 with Session() as s:
  if ok:
   values={'status':target,'phase':'done','finished_at':func.now()}
   if target=='failed' and row['status']=='running':values['error']='执行进程中断，未完成的试验已回滚，临时对象已清理'
   elif target in ('cancelled','timeout'):values['reason']=('执行超时' if target=='timeout' else '用户取消')+'，数据库执行已停止，临时对象已清理'
   n=s.execute(update(Experiment).where(Experiment.id==row['id'],Experiment.status==row['status']).values(**values)).rowcount
  else:n=s.execute(update(Experiment).where(Experiment.id==row['id'],Experiment.status==row['status']).values(status='cleanup_pending',reason='清理待处理：'+_brief(details))).rowcount
  s.commit()
 if n:log('worker','experiment.'+(target if ok else 'cleanup_pending'),str(row['id']))
def recover_active():
 """Startup and periodic janitor. Any active experiment seen while the main
 loop is idle has no live executor in this process, so it is safe to finalize.
 force_cleanup takes the serial lock first, which fails while another process
 legitimately runs a trial — its index is then left untouched."""
 with Session() as s:
  rows=s.execute(text("SELECT id,status,stop_kind,connection_id FROM experiments WHERE status IN ('running','cancelling','cleanup_pending')")).mappings().all()
 if not rows:return
 try:force_cleanup()
 except Exception:logging.exception('cleanup pass failed')
 for row in rows:
  try:_finalize_orphan(row)
  except Exception:logging.exception('finalize orphan %s failed',row['id'])
