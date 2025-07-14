/* src/pg_tkach_sheduler.c */

#include "c.h"
#include "postgres.h"
#include "fmgr.h"
#include "utils/guc.h"
#include "datatype/timestamp.h"
#include "utils/builtins.h"
#include "libpq/libpq-be.h"
#include "executor/spi.h"
#include "utils/elog.h"
#include "utils/timestamp.h"
#include <stdlib.h>

#include "pg_tkach_scheduler.h"
#include "task.h"
#include "ts_background_worker.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(ts_schedule);
PG_FUNCTION_INFO_V1(ts_unschedule);
PG_FUNCTION_INFO_V1(ts_check_shared_preload);


/*
 * проверка наличия расширения в shared_preload_libraries
 */
Datum
ts_check_shared_preload(PG_FUNCTION_ARGS)
{
    char *libs = GetConfigOption("shared_preload_libraries", true, false);
    bool found = (strstr(libs, "pg_tkach_scheduler") != NULL);
    PG_RETURN_BOOL(found);
}


/*
 * ts_schedule - запланировать задачу
 */
Datum
ts_schedule(PG_FUNCTION_ARGS)
{
    // индексы соответствующих параметров
    int indType = 0;
    int indCommand = 1;
    int indTimeNextExec = 2;
    int indExecInterval = 3;
    int indRepeatLimit = 4;
    int indUntil = 5;
    int indNote = 6;

    elog(LOG, "pg_tkach_scheduler ts_schedule");
    Task *task;

    PG_TRY();
    {
        text *taskTypeText;

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
            taskTypeText = PG_GETARG_TEXT_PP(indType);
        }

        elog(DEBUG1, "pg_tkach_scheduler ts_schedule 2");

        TaskType type = CStringToTaskType(text_to_cstring(taskTypeText));
        switch (type)
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
                elog(ERROR,
                     "exec_interval must be NOT NULL in repeat until task");
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
            elog(ERROR, "Invalid SQL command");

        /*
        * как говорится "We're ready to rock and roll..."
        */

        // получаем данные текущего пользователя и базу данных для него
        Port *myport = MyProcPort;
        const char *username = myport->user_name;
        const char *database = myport->database_name;

        task->command = command;
        task->type = type;
        task->exec_interval = exec_interval;
        task->time_next_exec = timeNextExec;
        task->repeat_limit = repeat_limit;
        task->until = timeUntil;
        task->note = note;
        task->username = username;
        task->database = database;

        PG_RETURN_INT64(ScheduleTask(task));
    }
    PG_CATCH();
    {
        if (task->command)
            pfree(task->command);
        if (task->note)
            pfree(task->note);
        if (task->username)
            pfree(task->username);
        if (task->database)
            pfree(task->database);

        PG_RE_THROW();
    }
    PG_END_TRY();

    PG_RETURN_INT64(-1);
}


/*
 * ts_unshedule - снять запланированную задачу
 * возвращает true - если удаление успешно
 */
Datum
ts_unschedule(PG_FUNCTION_ARGS)
{
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
bool
isValidQuery(const char *query)
{
    int spi_connected = false;
    SPIPlanPtr plan = NULL;
    bool result = false;
    MemoryContext oldcontext = CurrentMemoryContext;
    ErrorData *error = NULL;

    elog(DEBUG1, "pg_tkach_scheduler isValidQuery 1");

    // Сохраняем текущий контекст ошибок
    ErrorContextCallback *errcxt = error_context_stack;

    elog(DEBUG1, "pg_tkach_scheduler isValidQuery 2");

    PG_TRY();
    {
        elog(DEBUG1, "pg_tkach_scheduler isValidQuery 3");

        if (SPI_connect() != SPI_OK_CONNECT)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_FAILURE),
                     errmsg("SPI connection failed")));
        }
        spi_connected = true;

        elog(DEBUG1, "pg_tkach_scheduler isValidQuery 4");

        // Попытка подготовить план
        plan = SPI_prepare(query, 0, NULL);

        if (plan == NULL)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("pg_tkach_scheduler error preparing query")));
        }

        // План успешно подготовлен - запрос валиден
        result = true;
    }
    PG_CATCH();
    {
        // Переключаемся в безопасный контекст
        MemoryContextSwitchTo(oldcontext);

        // Сохраняем информацию об ошибке
        error = CopyErrorData();
        FlushErrorState();

        // Запрос невалиден, но это ожидаемо
        result = false;

        // для отладки
        elog(DEBUG2, "Query validation failed: %s", error->message);
    }
    PG_END_TRY();

    elog(DEBUG1, "pg_tkach_scheduler isValidQuery 5");

    // очистка ресурсов
    if (spi_connected)
    {
        if (plan != NULL)
        {
            SPI_freeplan(plan);
        }
        SPI_finish();
    }

    elog(DEBUG1, "pg_tkach_scheduler isValidQuery 6");

    // восстанавливаем контекст ошибок
    error_context_stack = errcxt;

    if (error)
    {
        FreeErrorData(error);
    }

    return result;
}

// bool
// isValidQuery(const char *query)
// {

//     int spi_connected = false;
//     SPIPlanPtr plan = NULL;

//     if (SPI_connect() != SPI_OK_CONNECT)
//     {
//         elog(ERROR, "failed to connect to SPI");
//         return false;
//     }
//     spi_connected = true;

//     PG_TRY();
//     {
//         // попытка подготовить план
//         plan = SPI_prepare(query, 0, NULL);

//         if (plan == NULL)
//         {
//             ereport(ERROR,
//                     (errcode(ERRCODE_SYNTAX_ERROR),
//                      errmsg("Error preparing request"),
//                      errdetail("SPI_prepare return NULL")));
//         }

//         if (SPI_keepplan(plan) < 0)
//         {
//             ereport(ERROR,
//                     (errcode(ERRCODE_INTERNAL_ERROR),
//                      errmsg("Error saving query plan")));
//         }
//     }
//     PG_CATCH();
//     {
//         ErrorData *error = CopyErrorData();
//         FlushErrorState();

//         if (spi_connected)
//         {
//             SPI_finish();
//         }

//         ereport(ERROR,
//                 (errcode(error->sqlerrcode),
//                  errmsg("Incorrect SQL query"),
//                  errdetail("%s", error->message),
//                  errhint("Query: %s", query)));

//         FreeErrorData(error);
//         return false; // только если ERROR обрабатывается выше
//     }
//     PG_END_TRY();

//     SPI_freeplan(plan);
//     SPI_finish();
//     return true;
// }