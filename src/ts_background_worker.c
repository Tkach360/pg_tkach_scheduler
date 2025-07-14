/* src/ts_background_worker.c */

#include "c.h"
#include "postgres.h"
#include "fmgr.h"

#include "catalog/pg_type_d.h"
#include "datatype/timestamp.h"
#include "nodes/pg_list.h"
#include "postmaster/bgworker.h"
#include "pgstat.h"
#include "storage/proc.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "executor/spi.h"
#include "utils/ps_status.h"

#include "task.h"
#include "ts_background_worker.h"

#define MAX_WAIT 5000 // время между проверками задач

/* TODO: сделать изменение базы данных */
char *TSTableName = "postgres"; // база данных с которой работаем по-умолчанию
static volatile sig_atomic_t isSigTerm = false;

PGDLLEXPORT void TSMain(Datum arg);


/*
 * обработка сигнала SIGTERM
 */
static void
handleSigterm(SIGNAL_ARGS)
{
    isSigTerm = true;

    // если MyProc != NULL, значит процесс уже инициализирован
    // и тот процесс может находиться в состоянии ожидания
    // поэтому нужно "разбудить" его, чтобы остановить
    if (MyProc != NULL)
    {
        SetLatch(&MyProc->procLatch);
    }
}


/*
 * эта функция вызывается инфраструктурой Postgres при загрузке расширения
 */
void
_PG_init()
{
    elog(DEBUG1, "start register background worker pg_tkach_scheduler");

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
 * Это главный цикл обработки задач для background worker
 * именно тут происходит выборка задач и их выполнение
 */
void
TSMain(Datum arg)
{
    elog(DEBUG1, "start TSMain pg_tkach_scheduler");

    pqsignal(SIGTERM, handleSigterm);
    BackgroundWorkerUnblockSignals();
    set_ps_display("pg_tkach_scheduler: initializing");

    BackgroundWorkerInitializeConnection(TSTableName, NULL, 0);
    elog(DEBUG1, "TSMain connection");

    // это чтобы pg_stat_ativity мог распознать worker-а
    pgstat_report_appname("pg_tkach_scheduler");

    // контекст памяти для обработки цикла выполнения задач
    MemoryContext TSMainLoopContext;
    TSMainLoopContext =
        AllocSetContextCreate(CurrentMemoryContext,
                              "pg_tkach_scheduler TSMain context",
                              ALLOCSET_DEFAULT_SIZES);

    elog(DEBUG1, "pg_tkach_scheduler started");


    while (!isSigTerm)
    {
        elog(DEBUG1, "pg_tkach_scheduler in TSMain loop");

        MemoryContextSwitchTo(TSMainLoopContext);
        CHECK_FOR_INTERRUPTS();

        PG_TRY();
        {
            List *taskList = GetCurrentTaskList(GetCurrentTimestamp());
            if (taskList != NIL)
            {
                ExecuteAllTask(taskList);
                UpdateTaskStatus(taskList);
            }
            list_free_deep(taskList);
        }
        PG_CATCH();
        {
            ErrorData *error = CopyErrorData();
            elog(WARNING, "Error in task processing: %s", error->message);
            FlushErrorState();
            FreeErrorData(error);
        }
        PG_END_TRY();

        MemoryContextReset(TSMainLoopContext);

        int rc = WaitLatch(MyLatch,
                           WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                           MAX_WAIT,
                           PG_WAIT_EXTENSION);

        ResetLatch(MyLatch);

        // Проверка на завершение работы
        if (rc & WL_POSTMASTER_DEATH)
            isSigTerm = true;

        elog(DEBUG1, "pg_tkach_scheduler out TSMain loop");
    }

    MemoryContextDelete(TSMainLoopContext);
    elog(DEBUG1, "pg_tkach_scheduler worker shutting down");
    //proc_exit(0);
}


/*
 * получить список задач, которые нужно сейчас выполнить по шаблону  расписания
 */
List *
GetCurrentTaskList(TimestampTz time)
{
    elog(DEBUG1, "pg_tkach_scheduler start GetCurrentTaskList");

    int ret;
    List *taskList = NIL;
    // стоит ли выводить ts.task в макрос или глобальную переменную?

    MemoryContext oldcontext = CurrentMemoryContext;

    StartTransactionCommand();

    PG_TRY();
    {
        PushActiveSnapshot(GetTransactionSnapshot());

        if (SPI_connect() != SPI_OK_CONNECT)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_FAILURE),
                     errmsg("SPI connection failed")));
        }

        // не произвожу выборку note, так как это поле не нужно для выполнения задачи
        const char *sql;
        sql = "SELECT task_id, command, type, exec_interval, time_next_exec, "
              "repeat_limit, until, username, database, note "
              "FROM ts.task WHERE date_trunc('minute', time_next_exec) = "
              "date_trunc('minute', $1);";

        Datum argValues[1];
        argValues[0] = TimestampTzGetDatum(time);
        Oid argTypes[1] = { TIMESTAMPTZOID };
        char argNulls[1] = { ' ' };

        ret = SPI_execute_with_args(
            sql,
            1,
            argTypes,
            argValues,
            argNulls,
            true,
            0); // ошибка TRAP: failed Assert("CurrentMemoryContext != ErrorContext"), File: "elog.c", Line: 1757, PID: 9379

        if (ret != SPI_OK_SELECT)
        {
            ereport(WARNING,
                    (errcode(ERRCODE_DATA_EXCEPTION),
                     errmsg("Failed to select tasks"),
                     errdetail("SPI error code: %d", ret)));
        }
        else
        {
            SPITupleTable *tuptable = SPI_tuptable;
            int count = SPI_processed;

            for (int i = 0; i < count; i++)
            {
                MemoryContextSwitchTo(oldcontext);
                Task *task = GetTaskRecordFromTuple(tuptable, i);
                taskList = lappend(taskList, task);
                MemoryContextSwitchTo(SPI_processed ? oldcontext
                                                    : CurrentMemoryContext);
            }
        }

        // if (ret != SPI_OK_SELECT)
        //     elog(LOG, "failed to SELECT while getting current tasks");

        // int count = SPI_processed;

        // if (count > 0)
        // {
        //     SPITupleTable *tuptable = SPI_tuptable;

        //     for (int i = 0; i < count; i++)
        //     {
        //         Task *record = GetTaskRecordFromTuple(tuptable, i);
        //         taskList = lappend(taskList, record);
        //     }
        // }
        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(oldcontext);
        ErrorData *error = CopyErrorData();

        if (ActiveSnapshotSet())
            PopActiveSnapshot();
        AbortCurrentTransaction();

        ereport(WARNING,
                (errcode(error->sqlerrcode),
                 errmsg("Failed to get task list"),
                 errdetail("%s", error->message)));

        FlushErrorState();
        FreeErrorData(error);
    }
    PG_END_TRY();
    elog(DEBUG1, "pg_tkach_scheduler end GetCurrentTaskList");
    return taskList;
}


/*
 * получить запись задачи из кортежа по индексу
 */
Task *
GetTaskRecordFromTuple(SPITupleTable *tuptable, int index)
{
    elog(DEBUG1, "pg_tkach_scheduler start GetTaskRecordFromTuple");

    TupleDesc tupdesc = tuptable->tupdesc;
    HeapTuple tuple = tuptable->vals[index];

    Task *task = palloc(sizeof(Task));
    bool isnull;

    /*
     * проверки на некорректный NULL не делаю, так как они были сделаны при вставке
     */

    task->task_id = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 1, &isnull));

    task->command =
        TextDatumGetCString(SPI_getbinval(tuple, tupdesc, 2, &isnull));

    task->type = CStringToTaskType(
        TextDatumGetCString(SPI_getbinval(tuple, tupdesc, 3, &isnull)));

    task->time_next_exec =
        DatumGetTimestampTz(SPI_getbinval(tuple, tupdesc, 5, &isnull));

    // лишь начальная инициализация,
    // далее в switch могут быть присвоены реальные значения
    task->exec_interval = NULL;
    task->repeat_limit = 0;
    task->until = 0;

    switch (task->type)
    {
    case Single:
        break;

    case Repeat:
        task->exec_interval =
            DatumGetIntervalP(SPI_getbinval(tuple, tupdesc, 4, &isnull));
        break;

    case RepeatLimit:
        task->exec_interval =
            DatumGetIntervalP(SPI_getbinval(tuple, tupdesc, 4, &isnull));
        task->repeat_limit =
            DatumGetInt64(SPI_getbinval(tuple, tupdesc, 6, &isnull));
        break;

    case RepeatUntil:
        task->exec_interval =
            DatumGetIntervalP(SPI_getbinval(tuple, tupdesc, 4, &isnull));
        task->until =
            DatumGetTimestampTz(SPI_getbinval(tuple, tupdesc, 7, &isnull));
        break;
    }

    // note может быть NULL
    Datum noteDatum;
    noteDatum = SPI_getbinval(tuple, tupdesc, 10, &isnull);
    if (!isnull)
        task->note = TextDatumGetCString(noteDatum);
    else
        task->note = NULL;

    task->username =
        TextDatumGetCString(SPI_getbinval(tuple, tupdesc, 8, &isnull));

    task->database =
        TextDatumGetCString(SPI_getbinval(tuple, tupdesc, 9, &isnull));

    elog(LOG, "pg_tkach_scheduler end GetTaskRecordFromTuple");
    return task;
}


/*
 * запустить задачи
 */
void
ExecuteAllTask(List *taskList)
{
    elog(DEBUG1, "pg_tkach_scheduler start ExecuteAllTask");
    ListCell *cell;

    foreach (cell, taskList)
    {
        Task *task = (Task *)lfirst(cell);
        ExecuteTask(task);
    }
    elog(DEBUG1, "pg_tkach_scheduler end ExecuteAllTask");
}


/*
 * выполнить одну задачу
 */
void
ExecuteTask(Task *task)
{
    elog(DEBUG1, "pg_tkach_scheduler start ExecuteTask");

    const char *command = task->command;

    MemoryContext TaskExecutionContext =
        AllocSetContextCreate(CurrentMemoryContext,
                              "pg_tkach_scheduler task execution",
                              ALLOCSET_DEFAULT_SIZES);

    MemoryContext oldContext = MemoryContextSwitchTo(TaskExecutionContext);

    PG_TRY();
    {
        // TODO: тут наверняка должна быть логика подстановки database и username
        int ret;

        PushActiveSnapshot(GetTransactionSnapshot());
        StartTransactionCommand();

        if ((ret = SPI_connect() != SPI_OK_CONNECT))
            elog(ERROR, "failed to connect to SPI while getting current tasks");

        ret = SPI_execute(command, false, 0);

        if (ret < 0)
        {
            ereport(WARNING,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Task execution failed: %s", command),
                     errdetail("SPI status: %d", ret)));
        }
        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
    }
    // если во время выполнения задачи произошла ошибка,
    // то восстанавливаем старый контекст и логируем ошибку
    PG_CATCH();
    {
        AbortCurrentTransaction();
        MemoryContextSwitchTo(oldContext);
        ErrorData *error = CopyErrorData();
        FlushErrorState();

        ereport(WARNING,
                (errcode(error->sqlerrcode),
                 errmsg("Task failed: %s", command),
                 errdetail("%s", error->message)));

        FreeErrorData(error);
    }
    PG_END_TRY();

    SPI_finish();

    // восстанавливаем контекст памяти и удаляем временный контекст
    MemoryContextSwitchTo(oldContext);
    MemoryContextDelete(TaskExecutionContext);
    elog(DEBUG1, "pg_tkach_scheduler end ExecuteTask");
}


/*
 * обновить время следующего выполнения задач
 * если задача больше никогда не выполнится, то она удалится
 */
void
UpdateTaskStatus(List *taskList)
{
    elog(DEBUG1, "pg_tkach_scheduler start UpdateTaskStatus");
    ListCell *cell;

    foreach (cell, taskList)
    {
        Task *task = (Task *)lfirst(cell);

        switch (task->type)
        {
        case (Single):
            DeleteTask(task->task_id);
            break;

        case (Repeat):
            UpdateTaskTimeNextExec(task->task_id, GetNewTimeNextExec(task));
            break;

        case (RepeatLimit):
            int repeat_limit = task->repeat_limit;
            repeat_limit--;
            if (repeat_limit == 0)
                DeleteTask(task->task_id);
            else
                UpdateRepeatLimitTask(task->task_id, repeat_limit);
            break;

        case (RepeatUntil):
            TimestampTz timeUntil = task->until;
            TimestampTz timeNextExec = GetNewTimeNextExec(task);

            if (timeUntil < timeNextExec)
                DeleteTask(task->task_id);
            else
                UpdateTaskTimeNextExec(task->task_id, timeNextExec);
            break;
        }
    }
    elog(DEBUG1, "pg_tkach_scheduler end UpdateTaskStatus");
}


/*
 * обновить время следующего выполнения задачи
 */
void
UpdateTaskTimeNextExec(int64 taskId, TimestampTz newNextTime)
{
    elog(DEBUG1, "pg_tkach_scheduler start UpdateTaskTimeNextExec");

    StartTransactionCommand();

    PG_TRY();
    {
        PushActiveSnapshot(GetTransactionSnapshot());
        if (SPI_connect() != SPI_OK_CONNECT)
            elog(ERROR, "failed to connect to SPI");

        const char *sql;
        sql = "UPDATE ts.task SET time_next_exec = $1 WHERE task_id = $2;";

        Datum argValues[2];
        Oid argTypes[2] = {
            TIMESTAMPTZOID,
            INT8OID,
        };
        char argNulls[2] = { '\0', '\0' };

        argValues[0] = TimestampTzGetDatum(newNextTime);
        argValues[1] = Int64GetDatum(taskId);

        int countArgs = 2; // количество аргументов для SPI_execute_with_args
        bool res =
            (SPI_OK_UPDATE ==
             SPI_execute_with_args(
                 sql, countArgs, argTypes, argValues, argNulls, false, 1));

        if (!res)
            elog(ERROR, "SPI_exec failed witch update time_next_exec");

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
    }
    PG_CATCH();
    {
        AbortCurrentTransaction();
        ErrorData *error = CopyErrorData();
        elog(LOG, error->message);
        FlushErrorState();
        FreeErrorData(error);
    }
    PG_END_TRY();

    elog(DEBUG1, "pg_tkach_scheduler end UpdateTaskTimeNextExec");
}


/*
 * обновить лимит выполнения задачи
 */
void
UpdateRepeatLimitTask(int64 taskId, int repeat_limit)
{
    elog(DEBUG1, "pg_tkach_scheduler start UpdateRepeatLimitTask");

    StartTransactionCommand();

    PG_TRY();
    {
        PushActiveSnapshot(GetTransactionSnapshot());
        if (SPI_connect() != SPI_OK_CONNECT)
            elog(ERROR, "failed to connect to SPI");

        const char *sql;
        sql = "UPDATE ts.task SET repeat_limit = $1 WHERE task_id = $2;";

        Datum argValues[2];
        Oid argTypes[2] = {
            TIMESTAMPTZOID,
            INT8OID,
        };
        char argNulls[2] = { '\0', '\0' };

        argValues[0] = Int64GetDatum(repeat_limit);
        argValues[1] = Int64GetDatum(taskId);

        int countArgs = 2; // количество аргументов для SPI_execute_with_args
        bool res =
            (SPI_OK_UPDATE ==
             SPI_execute_with_args(
                 sql, countArgs, argTypes, argValues, argNulls, false, 1));

        if (!res)
            elog(ERROR, "SPI_exec failed witch update repeat_limit");

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
    }
    PG_CATCH();
    {
        AbortCurrentTransaction();
        ErrorData *error = CopyErrorData();
        elog(LOG, error->message);
        FlushErrorState();
        FreeErrorData(error);
    }
    PG_END_TRY();

    elog(DEBUG1, "pg_tkach_scheduler end UpdateRepeatLimitTask");
}


/*
 * функция для удаления запланированной задачи
 */
bool
DeleteTask(int64 taskId)
{
    elog(DEBUG1, "pg_tkach_scheduler start DeleteTask");

    StartTransactionCommand();

    PG_TRY();
    {
        PushActiveSnapshot(GetTransactionSnapshot());
        if (SPI_connect() != SPI_OK_CONNECT)
            elog(ERROR, "failed to connect to SPI");

        char *sql;
        sprintf(sql, "DELETE FROM ts.task WHERE task_id = %ld", taskId);

        if (SPI_execute(sql, false, 0) != SPI_OK_DELETE)
        {
            ereport(ERROR, (errmsg("task delete error: %ld", taskId)));
            return false;
        }

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
    }
    PG_CATCH();
    {
        AbortCurrentTransaction();
        ErrorData *error = CopyErrorData();
        elog(LOG, error->message);
        FlushErrorState();
        FreeErrorData(error);
    }
    PG_END_TRY();

    elog(DEBUG1, "pg_tkach_scheduler end DeleteTask");
    return true;
}

/*
 * запланировать задачу с указанием всех параметров
 */
int64
ScheduleTask(Task *task)
{
    elog(DEBUG1, "pg_tkach_scheduler start ScheduleTask");
    int64 task_id = -1;
    bool transaction_ok = false;
    MemoryContext oldcontext = CurrentMemoryContext;

    StartTransactionCommand();

    PG_TRY();
    {
        PushActiveSnapshot(GetTransactionSnapshot());

        if (SPI_connect() != SPI_OK_CONNECT)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_FAILURE),
                     errmsg("SPI connection failed")));
        }

        const char *sql;
        sql = "INSERT INTO ts.task"
              "(type, command, exec_interval, time_next_exec, repeat_limit, "
              "until, "
              "note, username, database) "
              "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, &9);";

        Datum argValues[9];
        Oid argTypes[9] = {
            TEXTOID,        TEXTOID, TEXTOID, TIMESTAMPTZOID, INT8OID,
            TIMESTAMPTZOID, TEXTOID, TEXTOID, TEXTOID,
        };
        char argNulls[9] = { '\0', '\0', '\0', '\0', '\0',
                             '\0', '\0', '\0', '\0' };

        //MemoryContextSwitchTo(oldcontext);

        argValues[0] = CStringGetTextDatum(TaskTypeToCString(task->type));
        argValues[1] = CStringGetTextDatum(task->command);

        if (task->exec_interval == NULL)
            argNulls[2] = 'n'; // нужно, чтобы далее SPI_execute_with_args
                // поставил NULL на место аргумента
        else
            argValues[2] = IntervalPGetDatum(task->exec_interval);

        argValues[3] = TimestampTzGetDatum(task->time_next_exec);
        argValues[4] = Int64GetDatum(task->repeat_limit);

        if (task->until == 0)
            argNulls[5] = 'n';
        else
            argValues[5] = TimestampGetDatum(task->until);


        if (task->note == NULL)
            argNulls[6] = 'n';
        else
            argValues[6] = CStringGetTextDatum(task->note);

        argValues[7] = CStringGetTextDatum(task->username);
        argValues[8] = CStringGetTextDatum(task->database);

        //MemoryContextSwitchTo(TransactionCommandContext);
        MemoryContextSwitchTo(CurTransactionContext);

        int ret = SPI_execute_with_args(
            sql, 9, argTypes, argValues, argNulls, false, 1);

        if (ret != SPI_OK_INSERT || SPI_processed == 0)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_EXCEPTION),
                     errmsg("Failed to insert task"),
                     errdetail("SPI error code: %d", ret)));
        }

        if (SPI_tuptable != NULL && SPI_tuptable->tupdesc->natts >= 1)
        {
            task_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
                                                  SPI_tuptable->tupdesc,
                                                  1,
                                                  &transaction_ok));
            if (!transaction_ok)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_NO_DATA_FOUND),
                         errmsg("No task_id returned")));
            }
        }

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
        transaction_ok = true;
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(oldcontext);

        if (ActiveSnapshotSet())
            PopActiveSnapshot();

        AbortCurrentTransaction();

        ErrorData *error = CopyErrorData();
        elog(LOG, "Task scheduling failed: %s", error->message);
        FlushErrorState();
        FreeErrorData(error);
    }
    PG_END_TRY();

    elog(DEBUG1,
         "pg_tkach_scheduler: ScheduleTask completed, task_id=%ld",
         task_id);
    return task_id;
}
