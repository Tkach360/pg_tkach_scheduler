/* include/task.h */

#ifndef TASKH
#define TASKH

#include "c.h"
#include "postgres.h"
#include "utils/timestamp.h"
#include "utils/builtins.h"


/*
 * типы задач
 */
typedef enum {
	Single, 	 // задача, которая выполнится один раз
	Repeat, 	 // задача, которая будет повторяться бесконечно
	RepeatLimit, // задача, которая будет выполняться ограниченное число раз
	RepeatUntil, // задача, которая будет выполняться, пока не наступит определенное время
} TaskType;


/*
 * структура для хранения записи
 */
typedef struct Task
{
    int64 task_id;
    const char *command;
    TaskType type;
    Interval *exec_interval;
    TimestampTz time_next_exec;
    int64 repeat_limit;
    TimestampTz until;
    const char *note;
    const char *username;
    const char *database;

} Task;

TaskType CStringToTaskType(const char*);
const char* TaskTypeToCString(TaskType);
TimestampTz GetNewTimeNextExec(Task *);
TaskType Int32ToTaskType(int32 typeInt32);


#endif
