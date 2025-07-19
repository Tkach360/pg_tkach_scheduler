/* src/pg_tkach_sheduler.c */

#include "c.h"
#include "postgres.h"
#include "fmgr.h"
#include "postmaster/bgworker.h"
#include "utils/guc.h"
#include "datatype/timestamp.h"
#include "utils/builtins.h"
#include "libpq/libpq-be.h"
#include "executor/spi.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "utils/timestamp.h"
#include <stdlib.h>

#include "pg_tkach_scheduler.h"
#include "task.h"
#include "ts_background_worker.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(ts_schedule);
PG_FUNCTION_INFO_V1(ts_unschedule);


/*
 * эта функция вызывается инфраструктурой Postgres при загрузке расширения
 */
void
_PG_init()
{
    elog(DEBUG1, "start register background worker pg_tkach_scheduler");

    DefineCustomIntVariable(
        "pg_tkach_scheduler.task_check_interval",
        "Interval for checking new tasks (in seconds)",
        "Determines how often the background worker checks for new tasks to "
        "execute.",
        &task_check_interval,
        10,
        1,
        3600,
        PGC_SIGHUP,
        GUC_UNIT_S,
        NULL,
        NULL,
        NULL);

    BackgroundWorker worker;
    memset(&worker, 0, sizeof(BackgroundWorker));

    worker.bgw_flags =
        BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 5;

    worker.bgw_main_arg = Int32GetDatum(0);

    worker.bgw_notify_pid = 0;
    sprintf(worker.bgw_library_name, "pg_tkach_scheduler");
    snprintf(worker.bgw_name, BGW_MAXLEN, "pg_tkach_scheduler main");
    snprintf(worker.bgw_type, BGW_MAXLEN, "pg_tkach_scheduler main");
    sprintf(worker.bgw_function_name, "TSMain");

    RegisterBackgroundWorker(&worker);

    elog(DEBUG1, "end register background worker pg_tkach_scheduler");
}


/*
 * проверка наличия расширения в shared_preload_libraries
 */
static void
check_shared_preload()
{
    char *libs = GetConfigOption("shared_preload_libraries", true, false);
    if (!(strstr(libs, "pg_tkach_scheduler") != NULL))
        elog(ERROR, "pg_tkach_scheduler not found in shared_preload_libraries");
}


/*
 * ts_schedule - запланировать задачу
 */
Datum
ts_schedule(PG_FUNCTION_ARGS)
{
    check_shared_preload();

    // индексы соответствующих параметров
    int indType = 0;
    int indCommand = 1;
    int indTimeNextExec = 2;
    int indExecInterval = 3;
    int indRepeatLimit = 4;
    int indUntil = 5;
    int indNote = 6;

    elog(LOG, "pg_tkach_scheduler ts_schedule");
    Task *task = palloc(sizeof(Task));

    TaskType taskType;

    text *commandText;
    const char *command;

    Interval *exec_interval;

    TimestampTz timeNextExec;
    int64 repeat_limit = 0;
    TimestampTz timeUntil = 0;

    text *noteText;
    const char *note = NULL;

    /*
        * проверки, проверки и ещё раз проверки
        */

    elog(DEBUG1, "pg_tkach_scheduler ts_schedule 1");

    if (PG_ARGISNULL(indType))
        elog(ERROR, "task_type must be NOT NULL");
    else
    {
        elog(DEBUG1, "pg_tkach_scheduler ts_schedule taskTypeText");
        taskType = CStringToTaskType(DatumGetCString(
            DirectFunctionCall1(enum_out, PG_GETARG_DATUM(indType))));
        elog(DEBUG1, "TaskType - %d", taskType);
    }

    elog(DEBUG1, "pg_tkach_scheduler ts_schedule 2");

    switch (taskType)
    {
    case Single:
        break;

    case Repeat:
        if (PG_ARGISNULL(indExecInterval))
            elog(ERROR, "exec_interval must be NOT NULL in repeat task");
        else
            exec_interval = PG_GETARG_INTERVAL_P(indExecInterval);

        break;

    case RepeatLimit:
        if (PG_ARGISNULL(indExecInterval))
            elog(ERROR,
                 "exec_interval must be NOT NULL in repeat repeat_limit "
                 "task");
        else
            exec_interval = PG_GETARG_INTERVAL_P(indExecInterval);

        if (PG_ARGISNULL(indRepeatLimit))
            elog(ERROR,
                 "repeat_limit must be NOT NULL in "
                 "repeat_limit task");
        else
        {
            repeat_limit = PG_GETARG_INT64(indRepeatLimit);
            if (repeat_limit == 0)
                elog(ERROR,
                     "repeat_limit must not be 0 in repeat "
                     "repeat_limit task");
        }

        break;

    case RepeatUntil:
        if (PG_ARGISNULL(indExecInterval))
            elog(ERROR, "exec_interval must be NOT NULL in repeat until task");
        else
            exec_interval = PG_GETARG_INTERVAL_P(indExecInterval);

        if (PG_ARGISNULL(indUntil))
            elog(ERROR, "time_until must be NOT NULL in repeat until task");
        else
            timeUntil = PG_GETARG_TIMESTAMPTZ(indUntil);

        break;
    }

    elog(DEBUG1, "pg_tkach_scheduler ts_schedule 3");

    if (PG_ARGISNULL(indCommand))
        elog(ERROR, "command must be NOT NULL");
    else
    {
        commandText = PG_GETARG_TEXT_P(indCommand);
        command = text_to_cstring(commandText);
        elog(DEBUG1, "ts_schedule - command: %s", command);
    }

    elog(DEBUG1, "pg_tkach_scheduler ts_schedule 4");

    if (PG_ARGISNULL(indTimeNextExec))
        elog(ERROR, "time_next_exec must be NOT NULL");
    else
        timeNextExec = PG_GETARG_TIMESTAMPTZ(indTimeNextExec);

    elog(DEBUG1, "pg_tkach_scheduler ts_schedule 5");

    if (PG_ARGISNULL(indNote))
        noteText = NULL;
    else
    {
        noteText = PG_GETARG_TEXT_P(indNote);
        note = text_to_cstring(noteText);
    }

    elog(DEBUG1, "pg_tkach_scheduler ts_schedule 6");

    if (!isValidQuery(command))
    {
        elog(LOG, "Invalid SQL command");
        PG_RETURN_INT64(-1);
    }
    else
        elog(DEBUG1, "isValidQuery");

    /*
    * как говорится "We're ready to rock and roll..."
    */

    // получаем данные текущего пользователя и базу данных для него
    elog(DEBUG1, "pg_tkach_scheduler ts_schedule 7");
    Port *myport = MyProcPort;
    elog(DEBUG1, "pg_tkach_scheduler ts_schedule 8");
    const char *username = myport->user_name;
    const char *database = myport->database_name;
    elog(DEBUG1, "pg_tkach_scheduler ts_schedule 9");


    task->command = command;
    task->type = taskType;
    task->exec_interval = exec_interval;
    task->time_next_exec = timeNextExec;

    task->repeat_limit = repeat_limit;
    task->until = timeUntil;
    task->note = note;
    task->username = username;
    task->database = database;

    elog(DEBUG1, "Type - %d", task->type);


    int64 res = ScheduleTask(task);
    //int64 res = schedule_task();
    pfree(task);
    elog(DEBUG1, "pg_tkach_scheduler ts_schedule 11");

    PG_RETURN_INT64(res);
}


/*
 * ts_unshedule - снять запланированную задачу
 * возвращает true - если удаление успешно
 */
Datum
ts_unschedule(PG_FUNCTION_ARGS)
{
    check_shared_preload();
    elog(DEBUG1, "pg_tkach_scheduler ts_unschedule");

    int64 taskId;
    if (PG_ARGISNULL(0))
        elog(ERROR, "taskId must be NOT NULL");
    else
        taskId = PG_GETARG_INT64(0);

    PG_RETURN_BOOL(DeleteTask(taskId));
}


/*
 * функция для проверки корректности SQL запроса 
 */
static bool
isValidQuery(const char *sql)
{
    int ret;
    bool result = false;

    if ((ret = SPI_connect()) < 0)
    {
        elog(LOG, "SPI_connect failed");
        return false;
    }

    // Подготавливаем запрос
    elog(DEBUG1, "pg_tkach_scheduler isValidQuery 1");
    SPIPlanPtr plan = SPI_prepare(sql, 0, NULL);
    elog(DEBUG1, "pg_tkach_scheduler isValidQuery 2");

    if (plan != NULL)
    {
        result = true;
        elog(DEBUG1, "pg_tkach_scheduler isValidQuery 3");
        SPI_freeplan(plan);
        elog(DEBUG1, "pg_tkach_scheduler isValidQuery 4");
    }

    SPI_finish();

    elog(DEBUG1, "pg_tkach_scheduler isValidQuery 5");

    return result;
}