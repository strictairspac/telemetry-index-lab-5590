"""Controlled MySQL index trial. All dynamic identifiers are server-whitelisted."""
import hashlib,json,time,statistics,logging
from sqlalchemy import text
from src.config.database import engine
from src.constants.enums import COLUMNS
LOCK_NAME='index_trial_serial'
TRIAL_INDEX='trial_candidate'
class TrialInterrupted(Exception):pass
def prefix_score(columns,w):
 equality={'sensor_id'}|({'device_type'} if w.get('device_type') else set())
 score=0;matched=[];reason=[]
 for col in columns:
  if col in equality:score+=40;matched.append(col);reason.append(f'{col}: equality prefix +40')
  elif col=='recorded_at':score+=20;matched.append(col);reason.append('recorded_at: range +20; following columns cannot extend lookup');break
  else:reason.append(f'{col}: unmatched prefix stops lookup');break
 return {'score':min(100,score),'matched_prefix':matched,'reason':reason,'formula':'equality prefix +40/column, first range +20, max 100; heuristic only'}
def _timed(conn,query,params):
 samples=[];rows=[]
 for _ in range(5):
  start=time.perf_counter();rows=[dict(r) for r in conn.execute(text(query),params).mappings()];samples.append((time.perf_counter()-start)*1000)
 return rows,round(statistics.median(samples),4),[round(x,4) for x in samples]
def _write_probe(conn):
 t=time.perf_counter()
 conn.execute(text("INSERT INTO telemetry(sensor_id,recorded_at,device_type,value) VALUES (999,'2026-11-17T12:00:00','pump',42.5)"),[{} for _ in range(20)])
 elapsed=(time.perf_counter()-t)*1000;conn.rollback();return round(elapsed,4)
def _index_exists(conn):
 return conn.execute(text("SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='telemetry' AND index_name=:n"),{'n':TRIAL_INDEX}).scalar()>0
def _drop_index(conn):
 if _index_exists(conn):conn.execute(text(f'DROP INDEX {TRIAL_INDEX} ON telemetry'));conn.commit()
class Trial:
 """One interruptible execution. control carries set_phase/should_stop/set_connection_id/cleanup_started."""
 def __init__(self,snapshot,control):
  self.snapshot=snapshot;self.control=control;self.conn=None;self.connection_id=None;self.lock_held=False
 def _phase(self,name):
  self.control.set_phase(name)
  if self.control.should_stop():raise TrialInterrupted()
 def run(self):
  w=self.snapshot['workload'];columns=self.snapshot['candidate']['columns']
  if not columns or any(c not in COLUMNS for c in columns) or len(set(columns))!=len(columns):raise ValueError('invalid index columns')
  params={'sensor':w['sensor_id'],'start':w['start_at'],'end':w['end_at']}
  query='SELECT id,sensor_id,recorded_at,device_type,value FROM telemetry WHERE sensor_id=:sensor AND recorded_at>=:start AND recorded_at<=:end'
  if w.get('device_type'):query+=' AND device_type=:device';params['device']=w['device_type']
  query+=' ORDER BY id'
  self.conn=engine.connect()
  try:
   self.connection_id=self.conn.execute(text('SELECT CONNECTION_ID()')).scalar()
   self.control.set_connection_id(self.connection_id)
   self._phase('locking')
   if self.conn.execute(text(f"SELECT GET_LOCK('{LOCK_NAME}',30)")).scalar()!=1:raise RuntimeError('another trial holds lock')
   self.lock_held=True
   self._phase('baseline')
   _drop_index(self.conn)
   before=[dict(r) for r in self.conn.execute(text('EXPLAIN '+query),params).mappings()]
   rows,before_ms,before_samples=_timed(self.conn,query,params)
   write_before=_write_probe(self.conn)
   self._phase('indexing')
   self.conn.execute(text('CREATE INDEX '+TRIAL_INDEX+' ON telemetry ('+','.join('`'+c+'`' for c in columns)+')'));self.conn.commit()
   self.conn.execute(text('ANALYZE TABLE telemetry')).all()
   self._phase('measuring')
   after=[dict(r) for r in self.conn.execute(text('EXPLAIN '+query),params).mappings()]
   result,after_ms,after_samples=_timed(self.conn,query,params)
   write_after=_write_probe(self.conn)
   canonical=lambda records:hashlib.sha256(json.dumps(records,sort_keys=True).encode()).hexdigest()
   evidence={'prefix':prefix_score(columns,w),'query':query,'parameters':params,'rows':len(result),'before_plan':before,'after_plan':after,'before_median_ms':before_ms,'after_median_ms':after_ms,'before_samples_ms':before_samples,'after_samples_ms':after_samples,'insert20_before_ms':write_before,'insert20_after_ms':write_after,'equal_results':canonical(rows)==canonical(result),'checksum':canonical(result),'preview':result[:10],'limitations':['5000-row deterministic dataset; timings include Python driver and cache effects','20-row write probe is rolled back; sequence gaps are normal','heuristic prefix score is not MySQL optimizer cost; no guaranteed speedup']}
   self._cleanup()
   return evidence
  except TrialInterrupted:raise
  except Exception as exc:
   if self.control.should_stop():raise TrialInterrupted() from exc
   raise
  finally:
   try:self._cleanup()
   except Exception:logging.exception('trial cleanup incomplete; worker will retry')
 def _cleanup(self):
  """Idempotent best-effort: roll back the probe, drop the temp index, release the serial lock."""
  self.control.cleanup_started.set()
  if not self.conn:return
  self.control.set_phase('cleanup')
  try:self.conn.execute(text('SET SESSION lock_wait_timeout=5'))
  except Exception:return
  try:self.conn.rollback()
  except Exception:pass
  try:_drop_index(self.conn)
  finally:
   if self.lock_held:
    try:self.conn.execute(text(f"SELECT RELEASE_LOCK('{LOCK_NAME}')"));self.conn.commit()
    except Exception:pass
    self.lock_held=False
 def close(self):
  """Terminate the MySQL session instead of pooling it, so no lock or transaction can linger."""
  conn,self.conn=self.conn,None
  if conn is None:return
  try:conn.invalidate()
  except Exception:pass
  try:conn.close()
  except Exception:pass
def verify_clean(connection_id=None):
 """Confirm DB execution stopped, probe rolled back (no leaked probe rows) and
 temp index dropped. Uses only views visible to the trial user itself; the
 telemetry read is bounded so a lock holder defers—never hangs—confirmation.
 Any failure to confirm counts as not-clean (cleanup_pending), never raises."""
 with engine.connect() as c:
  try:
   c.execute(text('SET SESSION lock_wait_timeout=2'))
   active=0
   if connection_id:
    active=c.execute(text("SELECT COUNT(*) FROM information_schema.processlist WHERE ID=:i AND COMMAND NOT IN ('Sleep','Daemon','Connect')"),{'i':connection_id}).scalar()
   leaked=c.execute(text('SELECT COUNT(*) FROM telemetry WHERE sensor_id=999')).scalar()
   idx=c.execute(text("SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='telemetry' AND index_name=:n"),{'n':TRIAL_INDEX}).scalar()
   return (active==0 and leaked==0 and idx==0),{'query_active':active,'probe_rows_leaked':leaked,'leftover_index':idx}
  except Exception as exc:
   logging.info('cleanup verification inconclusive: %s',exc)
   return False,{'verification_blocked':1}
  finally:
   try:c.execute(text('SET SESSION lock_wait_timeout=DEFAULT'));c.commit()
   except Exception:pass
def force_cleanup():
 """Drop a leftover trial index and any leaked probe rows. Runs only under the
 serial lock, so a live trial's objects are never touched."""
 with engine.connect() as c:
  try:
   c.execute(text('SET SESSION lock_wait_timeout=5'))
   if c.execute(text(f"SELECT GET_LOCK('{LOCK_NAME}',3)")).scalar()!=1:return False,'试验锁被占用，跳过清理以保护进行中的试验'
   try:
    _drop_index(c)
    c.execute(text('DELETE FROM telemetry WHERE sensor_id=999'));c.commit()
   finally:
    c.execute(text(f"SELECT RELEASE_LOCK('{LOCK_NAME}')"));c.commit()
  finally:
   try:c.execute(text('SET SESSION lock_wait_timeout=DEFAULT'));c.commit()
   except Exception:pass
 return True,'临时索引与探针残留已清理'
