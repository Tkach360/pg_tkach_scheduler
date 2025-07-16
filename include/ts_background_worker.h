/* include/ts_background_worker.h */

#ifndef TS_BACKGROUND_WORKER
#define TS_BACKGROUND_WORKER

void TSMain(Datum);
void ExecuteAllTask(List *);
void ExecuteTask(Task *);
void UpdateTaskStatus(List *);
List *GetCurrentTaskList(TimestampTz);
Task *GetTaskRecordFromTuple(SPITupleTable *, int);
void UpdateTaskTimeNextExec(int64, TimestampTz);
void UpdateRepeatLimitTask(int64, int);
bool DeleteTask(int64);
int64 ScheduleTask(Task*);
void freeTaskList(List*);

#endif // TS_BACKGROUND_WORKER