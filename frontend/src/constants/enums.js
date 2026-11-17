export const ROLES={admin:'管理员',operator:'操作员',viewer:'观察员'};
export const STATUSES={queued:'等待执行',running:'试验中',cancelling:'取消中',cleanup_pending:'清理待处理',completed:'已完成',failed:'执行失败',cancelled:'已取消',timeout:'已超时'};
export const PHASES={queued:'排队中',locking:'等待试验锁',baseline:'基线测量',indexing:'创建索引',measuring:'候选测量',cleanup:'清理临时对象',done:'已结束'};
export const COLUMNS=['sensor_id','recorded_at','device_type','value'];
