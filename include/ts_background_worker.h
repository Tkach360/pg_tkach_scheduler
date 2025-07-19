/* include/ts_background_worker.h */

#ifndef TS_BACKGROUND_WORKER
#define TS_BACKGROUND_WORKER

#include "utils/guc.h"

extern int task_check_interval;

void TSMain(Datum);
static void ExecuteAllTask(List *);
static void ExecuteTask(Task *);
static void UpdateTaskStatus(List *);
static List *GetCurrentTaskList(TimestampTz);
static Task *GetTaskRecordFromTuple(SPITupleTable *, int);
static void UpdateTaskTimeNextExec(int64, TimestampTz);
static void UpdateRepeatLimitTask(int64, int);
static void freeTaskList(List*);

extern int64 ScheduleTask(Task*);
extern bool DeleteTask(int64);

#endif // TS_BACKGROUND_WORKER