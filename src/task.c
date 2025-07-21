
#include "task.h"
#include "postgres.h"
#include "utils/elog.h"
#include "utils/timestamp.h"
#include <string.h>


/*
 * распарсить тип из строки
 */
TaskType
CStringToTaskType(const char *type)
{
    if (strcmp(type, "single") == 0)
        return Single;
    else if (strcmp(type, "repeat") == 0)
        return Repeat;
    else if (strcmp(type, "repeat_limit") == 0)
        return RepeatLimit;
    else if (strcmp(type, "repeat_until") == 0)
        return RepeatUntil;
    else
        return Single; // на всякий случай

    // другим значением type быть не может
    // на это есть проверка в pg_tkach_scheduler--1.0.sql
}

/*
 * перевести тип записи в const char*
 */
const char *
TaskTypeToCString(TaskType type)
{
    switch (type)
    {
    case Single:
        return "single";
    case Repeat:
        return "repeat";
    case RepeatLimit:
        return "repeat_limit";
    case RepeatUntil:
        return "repeat_until";
    default:
        return "ERROR";
    }
}


/*
 * число в тип записи
 */
TaskType
Int32ToTaskType(int32 typeInt32)
{
    switch (typeInt32)
    {
    case 0:
        return Single;
    case 1:
        return Repeat;
    case 2:
        return RepeatLimit;
    case 3:
        return RepeatUntil;
    }
}


/*
 * получить новое время следующего выполнения задачи
 */
TimestampTz
GetNewTimeNextExec(Task *task)
{
    return DatumGetTimestampTz(
        DirectFunctionCall2(timestamptz_pl_interval,
                            TimestampTzGetDatum(task->time_next_exec),
                            PointerGetDatum(task->exec_interval)));
}
