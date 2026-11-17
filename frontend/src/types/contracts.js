/** @typedef {{id:number,name:string,sensor_id:number,start_at:string,end_at:string,device_type:string|null}} Workload */
/** @typedef {{id:number,name:string,workload_id:number,columns:string[]}} Candidate */
/** @typedef {{id:number,name:string,status:'queued'|'running'|'cancelling'|'cleanup_pending'|'completed'|'failed'|'cancelled'|'timeout',phase:'queued'|'locking'|'baseline'|'indexing'|'measuring'|'cleanup'|'done',reason:string|null,stop_kind:'user'|'timeout'|null,time_limit_seconds:number,started_at:string|null,finished_at:string|null,result:object|null}} Experiment */
export {};
