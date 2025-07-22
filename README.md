# PG_TKACH_SCHEDULER

Это расширение - планировщик для PostgreSQL. Оно предназначено для выполнения задач по заданному расписанию в автоматическом режиме. Планировщик поддерживает типы задач:
- `single` - задача, которая выполнится один раз
- `repeat` - задача, которая будет выполняться бесконечно с заданным периодом
- `repeat_limit` - задача, которая будет выполняться ограниченное число раз
- `repeat_until` - задача, которая будет выполняться до определенного времени 

## Как установить?
Для начала необходимо добавить `pg_tkach_scheduler` в `shared_preload_libraries`, после этого используйте обычный `CREATE EXTENSION`.

## Как использовать?
В качестве задач используются SQL запросы. Чтобы запланировать задачу, используйте функцию, соответствующую желаемому типу задачи:

```SQL
ts.schedule_single(	
    command TEXT,           -- SQL запрос
    time_exec TIMESTAMPTZ,  -- время выполнения
    note TEXT DEFAULT NULL  -- комментарий (опционально)
)


ts.schedule_repeat(
    command TEXT,               -- SQL запрос
    time_next_exec TIMESTAMPTZ, -- время следующего выполнения
    exec_interval INTERVAL,     -- интервал выполнения
    note TEXT DEFAULT NULL      -- комментарий (опционально)
)


ts.schedule_repeat_limit(
    command TEXT,               -- SQL запрос
    time_next_exec TIMESTAMPTZ, -- время следующего выполнения
    exec_interval INTERVAL,     -- интервал выполнения
    repeat_limit BIGINT,        -- количество выполнений
    note TEXT DEFAULT NULL      -- комментарий (опционально)
)


ts.schedule_repeat_until(
    command TEXT,               -- SQL запрос
    time_next_exec TIMESTAMPTZ, -- время следующего выполнения
    exec_interval INTERVAL,     -- интервал выполнения
    until TIMESTAMPTZ,          -- до какого момента выполнять задачу
    note TEXT DEFAULT NULL      -- комментарий (опционально)
)
```

Вообще, описанные выше функции - это просто более удобные обертки, над функцией `ts.schedule`, описанной ниже
```SQL
ts.schedule(
    type ts.TASK_TYPE,                   -- тип задачи
    command TEXT,                        -- SQL запрос
    time_next_exec TIMESTAMPTZ,          -- время следующего выполнения
    exec_interval INTERVAL DEFAULT NULL, -- интервал выполнения
    repeat_limit BIGINT DEFAULT NULL,    -- количество выполнений
    until TIMESTAMPTZ DEFAULT NULL,      -- до какого момента выполнять задачу
    note TEXT DEFAULT NULL               -- комментарий (опционально)
)
```

Все актуальные задачи (те, которые ещё выполнятся) хранятся в таблице `ts.task`. В ней можно просматривать время следующего выполнения задачи. Если задача больше не выполнится, она удаляется.