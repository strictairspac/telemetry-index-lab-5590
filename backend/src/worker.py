import time,logging,redis,threading
from pathlib import Path
from src.config.settings import REDIS_URL
from src.config.database import Session
from src.config.migrate import ensure_schema
from src.models.experiment import Experiment
from src.services.index_algorithm import Trial,TrialInterrupted
from src.services import lifecycle
logging.basicConfig(level=logging.INFO)
q=redis.Redis.from_url(REDIS_URL)
def process(run_id):
 control=lifecycle.TrialControl(run_id)
 if not lifecycle.claim(run_id):return
 watcher=threading.Thread(target=lifecycle.watch,args=(control,),daemon=True)
 watcher.start()
 trial=None
 try:
  with Session() as s:snapshot=s.get(Experiment,run_id).snapshot
  trial=Trial(snapshot,control)
  result=trial.run()
 except TrialInterrupted:
  lifecycle.finalize_stop(run_id,control)
 except Exception as exc:
  logging.exception('trial failed')
  lifecycle.finalize_failure(run_id,control,exc)
 else:
  if not lifecycle.complete(run_id,result):lifecycle.finalize_stop(run_id,control)
 finally:
  control.done.set()
  watcher.join(timeout=5)
  if trial:trial.close()
try:
 ensure_schema()
 lifecycle.recover_active()
except Exception:logging.exception('startup recovery failed')
n=0
while True:
 Path('/tmp/worker-heartbeat').touch()
 try:
  n+=1
  if n%5==0:lifecycle.recover_active()
  item=q.blpop('index-trials',timeout=2)
  if not item:continue
  process(int(item[1]))
 except Exception:logging.exception('worker loop');time.sleep(2)
