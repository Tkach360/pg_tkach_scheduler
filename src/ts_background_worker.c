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
#include "utils/palloc.h"
#include "utils/timestamp.h"
#include "executor/spi.h"
#include "utils/ps_status.h"

#include "task.h"
#include "ts_background_worker.h"

int task_check_interval = 10;

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

    elog(DEBUG1, "pg_tkach_scheduler started");


    while (!isSigTerm)
    {
        elog(DEBUG1, "pg_tkach_scheduler in TSMain loop");

        //MemoryContextSwitchTo(TSMainLoopContext);
        CHECK_FOR_INTERRUPTS();

        StartTransactionCommand();

        MemoryContext caller_ctx = CurrentMemoryContext;
        MemoryContext sched_ctx =
            AllocSetContextCreate(TopMemoryContext,
                                  "pg_tkach_scheduler context",
                                  ALLOCSET_DEFAULT_SIZES);
        MemoryContextSwitchTo(sched_ctx);

        List *taskList = GetCurrentTaskList(GetCurrentTimestamp());

        CommitTransactionCommand();

        MemoryContextSwitchTo(caller_ctx);

        elog(DEBUG1, "List length3: %d", list_length(taskList));

        if (taskList != NIL)
        {
            ExecuteAllTask(taskList);
            UpdateTaskStatus(taskList);
        }
        freeTaskList(taskList);
        MemoryContextDelete(sched_ctx);

        pg_usleep(task_check_interval * 1000000L);

        elog(DEBUG1, "pg_tkach_scheduler out TSMain loop");
    }

    //MemoryContextDelete(TSMainLoopContext);
    elog(DEBUG1, "pg_tkach_scheduler worker shutting down");
    //proc_exit(0);
}


/*
 * получить список задач, которые нужно сейчас выполнить по шаблону  расписания
 */
static List *
GetCurrentTaskList(TimestampTz time)
{
    elog(DEBUG1, "pg_tkach_scheduler start GetCurrentTaskList");

    int ret;
    List *taskList = NIL;
    // стоит ли выводить ts.task в макрос или глобальную переменную?

    //StartTransactionCommand();
    MemoryContext oldcontext = CurrentMemoryContext;

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

    ret = SPI_execute_with_args(sql, 1, argTypes, argValues, argNulls, true, 0);

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

            elog(DEBUG1, "GetCurrentTaskList task - %s", task->command);

            //taskList = lappend(taskList, GetTaskRecordFromTuple(tuptable, i));
            taskList = lappend(taskList, task);
        }
    }

    elog(DEBUG1, "List lenght1: %d", list_length(taskList)); // одно значение

    SPI_finish();
    PopActiveSnapshot();
    //CommitTransactionCommand();

    MemoryContextSwitchTo(oldcontext);
    elog(DEBUG1,
         "List lenght2: %d",
         list_length(taskList)); // совсем другое значение

    elog(DEBUG1, "pg_tkach_scheduler end GetCurrentTaskList");
    return taskList;
}


/*
 * получить запись задачи из кортежа по индексу
 */
static Task *
GetTaskRecordFromTuple(SPITupleTable *tuptable, int index)
{
    elog(DEBUG1, "pg_tkach_scheduler start GetTaskRecordFromTuple");

    TupleDesc tupdesc = tuptable->tupdesc;
    HeapTuple tuple = tuptable->vals[index];

    Task *task = palloc(sizeof(Task));

    elog(DEBUG1, "pg_tkach_scheduler start GetTaskRecordFromTuple 1");
    bool isnull;

    /*
     * проверки на некорректный NULL не делаю, так как они были сделаны при вставке
     */

    task->task_id = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 1, &isnull));
    elog(DEBUG1, "pg_tkach_scheduler start GetTaskRecordFromTuple 2");

    task->command =
        pstrdup(TextDatumGetCString(SPI_getbinval(tuple, tupdesc, 2, &isnull)));
    elog(DEBUG1, "pg_tkach_scheduler start GetTaskRecordFromTuple 3");

    task->type = CStringToTaskType(DatumGetCString(DirectFunctionCall1(
        enum_out, SPI_getbinval(tuple, tupdesc, 3, &isnull))));

    // task->type = CStringToTaskType(
    //     TextDatumGetCString(SPI_getbinval(tuple, tupdesc, 3, &isnull)));
    elog(DEBUG1, "pg_tkach_scheduler start GetTaskRecordFromTuple 4");

    task->time_next_exec =
        DatumGetTimestampTz(SPI_getbinval(tuple, tupdesc, 5, &isnull));
    elog(DEBUG1, "pg_tkach_scheduler start GetTaskRecordFromTuple 5");

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
    elog(DEBUG1, "pg_tkach_scheduler start GetTaskRecordFromTuple 6");

    // note может быть NULL
    Datum noteDatum;
    noteDatum = SPI_getbinval(tuple, tupdesc, 10, &isnull);
    if (!isnull)
        task->note = pstrdup(TextDatumGetCString(noteDatum));
    else
        task->note = NULL;
    elog(DEBUG1, "pg_tkach_scheduler start GetTaskRecordFromTuple 7");

    task->username =
        pstrdup(TextDatumGetCString(SPI_getbinval(tuple, tupdesc, 8, &isnull)));

    task->database =
        pstrdup(TextDatumGetCString(SPI_getbinval(tuple, tupdesc, 9, &isnull)));

    elog(LOG, "pg_tkach_scheduler end GetTaskRecordFromTuple");
    return task;
}


/*
 * запустить задачи
 */
static void
ExecuteAllTask(List *taskList)
{
    elog(DEBUG1, "pg_tkach_scheduler start ExecuteAllTask");
    ListCell *cell; // = palloc(sizeof(ListCell));

    elog(DEBUG1, "List lenght: %d", list_length(taskList));

    foreach (cell, taskList)
    {
        Task *task = (Task *)lfirst(cell);
        StartTransactionCommand();
        ExecuteTask(task);
        CommitTransactionCommand();
    }
    //pfree(cell);
    elog(DEBUG1, "pg_tkach_scheduler end ExecuteAllTask");
}


/*
 * выполнить одну задачу
 */
static void
ExecuteTask(Task *task)
{
    elog(DEBUG1, "pg_tkach_scheduler start ExecuteTask");

    const char *command = task->command;

    // TODO: тут наверняка должна быть логика подстановки database и username
    int ret;

    // StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

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
    // CommitTransactionCommand();

    // восстанавливаем контекст памяти и удаляем временный контекст
    elog(DEBUG1, "pg_tkach_scheduler end ExecuteTask");
}


/*
 * обновить время следующего выполнения задач
 * если задача больше никогда не выполнится, то она удалится
 */
static void
UpdateTaskStatus(List *taskList)
{
    elog(DEBUG1, "pg_tkach_scheduler start UpdateTaskStatus");
    ListCell *cell;

    foreach (cell, taskList)
    {
        Task *task = (Task *)lfirst(cell);

        //StartTransactionCommand();
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
        //CommitTransactionCommand();
    }
    elog(DEBUG1, "pg_tkach_scheduler end UpdateTaskStatus");
}


/*
 * обновить время следующего выполнения задачи
 */
static void
UpdateTaskTimeNextExec(int64 taskId, TimestampTz newNextTime)
{
    elog(DEBUG1, "pg_tkach_scheduler start UpdateTaskTimeNextExec");

    // StartTransactionCommand();

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
    bool res = (SPI_OK_UPDATE ==
                SPI_execute_with_args(
                    sql, countArgs, argTypes, argValues, argNulls, false, 1));

    if (!res)
        elog(ERROR, "SPI_exec failed witch update time_next_exec");

    SPI_finish();
    PopActiveSnapshot();
    // CommitTransactionCommand();

    elog(DEBUG1, "pg_tkach_scheduler end UpdateTaskTimeNextExec");
}


/*
 * обновить лимит выполнения задачи
 */
static void
UpdateRepeatLimitTask(int64 taskId, int repeat_limit)
{
    elog(DEBUG1, "pg_tkach_scheduler start UpdateRepeatLimitTask");

    // StartTransactionCommand();

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
    bool res = (SPI_OK_UPDATE ==
                SPI_execute_with_args(
                    sql, countArgs, argTypes, argValues, argNulls, false, 1));

    if (!res)
        elog(ERROR, "SPI_exec failed witch update repeat_limit");

    SPI_finish();
    PopActiveSnapshot();
    // CommitTransactionCommand();

    elog(DEBUG1, "pg_tkach_scheduler end UpdateRepeatLimitTask");
}


/*
 * функция для удаления запланированной задачи
 */
extern bool
DeleteTask(int64 taskId)
{
    elog(DEBUG1, "pg_tkach_scheduler start DeleteTask");

    StartTransactionCommand(); //
    PushActiveSnapshot(GetTransactionSnapshot());
    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "failed to connect to SPI");

    char sql[256];
    snprintf(
        sql, sizeof(sql), "DELETE FROM ts.task WHERE task_id = %ld", taskId);

    if (SPI_execute(sql, false, 0) != SPI_OK_DELETE)
    {
        SPI_finish();
        PopActiveSnapshot();
        AbortCurrentTransaction();
        ereport(ERROR, (errmsg("task delete error: %lld", (long long)taskId)));
        return false;
    }

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();

    elog(DEBUG1, "pg_tkach_scheduler end DeleteTask");
    return true;
}

/*
 * запланировать задачу с указанием всех параметров
 */
extern int64
ScheduleTask(Task *task)
{
    elog(DEBUG1, "pg_tkach_scheduler start ScheduleTask");
    int64 task_id = -1;
    bool transaction_ok = false;

    //StartTransactionCommand(); //
    elog(DEBUG1, "pg_tkach_scheduler ScheduleTask 1");

    PushActiveSnapshot(GetTransactionSnapshot());
    elog(DEBUG1, "pg_tkach_scheduler ScheduleTask 2");

    if (SPI_connect() != SPI_OK_CONNECT)
    {
        elog(LOG,
             "SPI connection failed: %d",
             errcode(ERRCODE_CONNECTION_FAILURE));
        return -1;
    }

    elog(DEBUG1, "pg_tkach_scheduler ScheduleTask 3");

    const char *sql;
    sql = "INSERT INTO ts.task"
          "(type, command, exec_interval, time_next_exec, repeat_limit, "
          "until, note, username, database) "
          "VALUES ($1::ts.TASK_TYPE, $2::TEXT, $3::INTERVAL, $4::TIMESTAMPTZ, "
          "$5::BIGINT, $6::TIMESTAMP, $7::TEXT, $8::TEXT, $9::TEXT) RETURNING "
          "task_id;";

    Datum argValues[9];
    Oid argTypes[9] = {
        TEXTOID,        TEXTOID, INTERVALOID, TIMESTAMPTZOID, INT8OID,
        TIMESTAMPTZOID, TEXTOID, TEXTOID,     TEXTOID,
    };
    char argNulls[9] = { '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0' };

    elog(DEBUG1, "pg_tkach_scheduler ScheduleTask before TaskType");
    elog(DEBUG1,
         "pg_tkach_scheduler ScheduleTask - %s",
         TaskTypeToCString(task->type));
    argValues[0] = CStringGetTextDatum(TaskTypeToCString(task->type)); //
    elog(DEBUG1, "pg_tkach_scheduler ScheduleTask after TaskType");
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

    int ret =
        SPI_execute_with_args(sql, 9, argTypes, argValues, argNulls, false, 1);

    if (ret != SPI_OK_INSERT_RETURNING || SPI_processed == 0)
    {
        ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                 errmsg("Failed to insert task"),
                 errdetail("SPI error code: %d", ret)));
    }

    if (SPI_processed > 0)
    {
        HeapTuple tuple = SPI_tuptable->vals[0];
        TupleDesc tupdesc = SPI_tuptable->tupdesc;

        bool isnull;
        Datum id_datum = SPI_getbinval(tuple, tupdesc, 1, &isnull);

        if (!isnull)
        {
            task_id = DatumGetInt64(id_datum);
        }
    }
    else
        elog(DEBUG1, "failed get task_id");

    SPI_finish();
    PopActiveSnapshot();

    transaction_ok = true;

    elog(DEBUG1,
         "pg_tkach_scheduler: ScheduleTask completed, task_id=%ld",
         task_id);
    return task_id;
}

static void
freeTaskList(List *list)
{
    ListCell *cell;
    foreach (cell, list)
    {
        Task *task = (Task *)lfirst(cell);
        pfree((void *)task->command);
        pfree((void *)task->database);
        pfree((void *)task->note);
        pfree((void *)task->username);
        pfree(task);
    }
    list_free(list);
}
