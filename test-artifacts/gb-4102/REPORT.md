# gb-4102 验证报告 — 试验取消与执行时限

日期 2026-11-17。环境：MySQL 8.0.40、Redis 7.4、FastAPI backend、worker（本地进程等价 compose 五容器；`WORKER_KILL_CMD/START_CMD` 在 compose 下换成 `docker compose kill -s SIGKILL worker` / `docker compose up -d worker`）。

运行：`./run_all.sh`（六个脚本顺序执行）。结果：**85 项检查全部通过，0 失败**。

## 场景与结论

### 1. 锁等待取消（test_1_lock_wait_cancel.py，27 项）
三种锁形态下取消运行中的试验：A 命名锁（GET_LOCK 被占用）、B 表写锁（LOCK TABLES WRITE 阻塞基线 EXPLAIN）、C 表读锁（阻塞写探针）。
- 三种形态下，取消后数据库执行都在 10 秒内停止（watcher KILL QUERY 中断锁等待，实测约 1 秒内）。
- A/C：清理可在持锁期间确认，10 秒内直接到「已取消」。
- B：写锁持有期间无法确认探针回滚，状态保持「取消中/清理待处理」，不出现假终态；UNLOCK 后 janitor 在数秒内完成清理并转「已取消」。
- 全部确认：临时索引已清理、无泄漏的探针行（sensor_id=999）、试验连接无活动语句。

### 2. 取消与完成竞争（test_2 + test_2b，31 项）
- 以实测试验时长（约 0.26s）的 0.4–1.5 倍延迟扫掠取消点：每次只出现一个终态；被接受的取消最终都是「已取消」，且 2 秒观察窗内终态稳定，无临时索引残留。
- 完成后才到的取消返回 409，已完成状态不受影响。
- 机制级（直接调用应用 `lifecycle.complete()`）：对排队取消与运行取消的试验，旧执行者迟到的成功写入均失败，记录保持「已取消」且无结果。

### 3. 执行中断恢复（test_3_crash_recovery.py，12 项）
- worker 在试验等待锁期间被 SIGKILL：重启后孤儿 running 试验被标记 failed（错误信息含「中断」），后续试验正常完成。
- 清理中断（cleanup_pending + 遗留 trial_candidate 索引，worker 停止期间构造）：启动恢复删除遗留索引并把取消推进到「已取消」。
- 索引归属：他人持有串行锁时 `force_cleanup()` 拒绝执行且遗留索引保持不动；锁可归因后才清理——不误删其他试验的索引。

### 4. 执行时限（test_4_timeout.py，9 项）
- 10 秒时限的试验被锁阻塞：由 watcher 在时限处停止（远早于 GET_LOCK 自身 30 秒超时），终态「已超时」，原因含「时限/超时」，数据库执行确认停止、无残留。
- 时限充足的正常试验照常完成。

### 5. 排队取消（test_5_queued_cancel.py，6 项）
- worker 被占满时取消排队试验：立即转「已取消」（不触碰数据库），started_at/result 保持空；重复取消返回 409。
- 队列排空后该试验也从未执行（worker 弹出旧队列项时 claim 条件更新失败即跳过）。

## 关键实现点
- 状态机全部走条件 UPDATE（queued→cancelled/running、running→cancelling/completed/failed、cancelling→cancelled/timeout/cleanup_pending），并发下唯一终态。
- watcher 线程 0.5s 轮询取消标记与 `started_at + time_limit_seconds` 时限，KILL QUERY 可中断 GET_LOCK/MDL/行锁等待。
- 终态发布前验证：processlist 无活动语句、无 sensor_id=999 泄漏探针行、无 trial_candidate 索引；验证读全部有 lock_wait_timeout 上限，确认不了就转 cleanup_pending 由 janitor 重试（绝不挂死）。
- 试验连接用完即 invalidate（不回池），锁与事务不可能跨试验残留。
- 迁移：`src/config/migrate.py` 幂等 ALTER 旧卷并回填终态行的 phase，已验证 v1→v2 数据保留。
