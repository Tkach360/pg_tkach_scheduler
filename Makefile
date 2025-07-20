# Makefile

EXTENSION = pg_tkach_scheduler

MODULE_big = $(EXTENSION)
OBJS = $(patsubst %.c,%.o,$(wildcard src/*.c))

DATA = $(EXTENSION)--1.0.sql

HDRS = $(wildcard include/*.h)

override CPPFLAGS += -I$(CURDIR)/include

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/$(EXTENSION)
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
