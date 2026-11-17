"""Idempotent schema upgrades for existing volumes. Fresh installs get the full
schema from Base.metadata.create_all; older volumes are altered in place."""
import logging
from sqlalchemy import text
from src.config.database import engine
EXPERIMENT_COLUMNS={
 'time_limit_seconds':'INT NOT NULL DEFAULT 120',
 'phase':"VARCHAR(20) NOT NULL DEFAULT 'queued'",
 'reason':'VARCHAR(500) NULL',
 'stop_kind':'VARCHAR(20) NULL',
 'started_at':'DATETIME NULL',
 'finished_at':'DATETIME NULL',
 'connection_id':'INT NULL',
}
def ensure_schema():
 with engine.connect() as c:
  table=c.execute(text("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='experiments'")).scalar()
  if not table:return
  existing={r[0] for r in c.execute(text("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='experiments'"))}
  for name,ddl in EXPERIMENT_COLUMNS.items():
   if name not in existing:
    logging.info('migrating experiments: add column %s',name)
    c.execute(text(f'ALTER TABLE experiments ADD COLUMN {name} {ddl}'))
  c.execute(text("UPDATE experiments SET phase='done' WHERE status IN ('completed','failed','cancelled','timeout') AND phase='queued'"))
  c.commit()
