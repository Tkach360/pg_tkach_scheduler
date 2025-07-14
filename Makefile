# Makefile

EXTENSION = pg_tkach_scheduler

MODULE_big = $(EXTENSION)
OBJS = $(patsubst %.c,%.o,$(wildcard src/*.c))

DATA = $(EXTENSION)--1.0.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

HDRS = $(wildcard include/*.h)

override CPPFLAGS += -I$(CURDIR)/include
#override CPPFLAGS += -I$(libdir)/pg_tkach_scheduler/include
#override CPPFLAGS += -I$(top_srcdir)/sum_school_2025/pg_tkach_scheduler/include