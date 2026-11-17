import redis
from fastapi import HTTPException
from sqlalchemy import update,func
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models.workload import Workload
from src.models.candidate import Candidate
from src.models.experiment import Experiment
from src.utils.serialization import record
queue=redis.Redis.from_url(REDIS_URL)
def list_items():
 with Session() as s:return [record(r) for r in s.query(Experiment).order_by(Experiment.id.desc()).limit(100).all()]
def detail(item_id):
 with Session() as s:
  row=s.get(Experiment,item_id)
  if not row:raise HTTPException(404,'未找到试验')
  return record(row)
def create(data):
 with Session() as s:
  c=s.get(Candidate,data.candidate_id)
  if not c:raise HTTPException(404,'未找到候选')
  w=s.get(Workload,c.workload_id)
  row=Experiment(name=data.name,candidate_id=c.id,workload_id=w.id,status='queued',time_limit_seconds=data.time_limit_seconds,snapshot={'workload':record(w),'candidate':record(c)})
  s.add(row);s.commit();s.refresh(row);result=record(row)
 try:queue.rpush('index-trials',result['id'])
 except redis.RedisError:
  with Session() as s:r=s.get(Experiment,result['id']);r.status='failed';r.error='队列不可用，请创建新试验';s.commit()
  raise HTTPException(503,'队列不可用，请稍后重新执行')
 return result
def cancel(item_id):
 """Atomic cancel transitions; returns (record, changed). Concurrent completion
 keeps its own terminal state, in which case the cancel is rejected with 409."""
 with Session() as s:
  row=s.get(Experiment,item_id)
  if not row:raise HTTPException(404,'未找到试验')
  if row.status=='queued':
   n=s.execute(update(Experiment).where(Experiment.id==item_id,Experiment.status=='queued').values(status='cancelled',stop_kind='user',reason='排队中取消，未占用数据库',finished_at=func.now())).rowcount
   s.commit()
   if n:
    try:queue.lrem('index-trials',0,item_id)
    except redis.RedisError:pass
    return detail(item_id),True
  if row.status=='running':
   n=s.execute(update(Experiment).where(Experiment.id==item_id,Experiment.status=='running').values(status='cancelling',stop_kind='user',reason='用户请求取消，正在停止数据库执行')).rowcount
   s.commit()
   if n:return detail(item_id),True
  s.expire_all();row=s.get(Experiment,item_id)
  if row.status in ('cancelling','cleanup_pending'):return record(row),False
  raise HTTPException(409,f'试验已结束（{row.status}），无法取消')
