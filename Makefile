LIBUV_BRANCH=v1.18.0

# Compiler
CC = gcc

# Compiler flags
CFLAGS_DEBUG = -W -Wall -g -I./include -DDEBUG
CFLAGS_RELEASE = -W -Wall -O3 -I./include -DRELEASE -DLINUX

ifeq ($(BUILD_TYPE), debug)
	CFLAGS = $(CFLAGS_DEBUG)
else
	CFLAGS = $(CFLAGS_RELEASE)
endif

# Linker flags - Note: -L and -l flags are now used for linking the executable
LDFLAGS = -L./libs/lsmt -llsmt \
					-L./libs/lmdb -llmdb \
					-L./libs/raft -lraft \
					-L./libs/libuv -luv \
					-L./libs/tpl/ -ltpl \
					-lm -lpthread -lrt 

# Source files
SRC = $(wildcard src/*.c)
INCLUDED_SRC = src/usage.c src/parse_addr.c

# Separate main.c from other source files which will form the library
MAIN_SRC = src/main.c
APP_SRC = $(filter-out $(MAIN_SRC) $(INCLUDED_SRC), $(SRC))

# Object files for the application/library
APP_OBJ = $(patsubst %.c, build/%.o, $(APP_SRC))

# Object file for the main entry point
MAIN_OBJ = build/src/main.o

# Executable targets
EXEC = build/ddb

# Default target: build the main executable
main: $(EXEC)


# Link main executable against the static library
$(EXEC): $(MAIN_OBJ) $(APP_OBJ) 
	$(CC) $^ -o $@ $(LDFLAGS)

# --- Compilation Rules ---

# Compile source files to object files
build/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

# --- Utility Targets ---

clean:
	rm -rf build/*

debug: BUILD_TYPE=debug
debug: main

release: BUILD_TYPE=release
release: main

.PHONY: main clean debug release 

lsmt:
	cd deps/lsmt && make static
	mkdir -p include/lsmt
	mkdir -p libs/lsmt
	cp deps/lsmt/include/*.h ./include/lsmt/ 
	cp deps/lsmt/build/lib/liblsmt.a ./libs/lsmt
.PHONY : lsmt

tpl:
	cd deps/tpl && make
	mkdir -p include/tpl
	mkdir -p libs/tpl
	cp deps/tpl/*.h ./include/tpl/ 
	cp deps/tpl/build/lib/libtpl.a ./libs/tpl/
.PHONY : tpl

lmdb:
	cd deps/lmdb && make
	mkdir -p include/lmdb
	mkdir -p libs/lmdb
	cp deps/lmdb/*.h ./include/lmdb/ 
	cp -r deps/lmdb/lmdb/ ./include/lmdb/ 
	cp deps/lmdb/build/lib/liblmdb.a ./libs/lmdb/
.PHONY : lmdb

raft_build:
	cd deps/raft && make all
	mkdir -p include/raft
	mkdir -p libs/raft
	cp deps/raft/*.h ./include/raft/ 
	cp deps/raft/build/lib/libraft.a ./libs/raft/
.PHONY : raft_build

raft_fetch:
	if test -e deps/raft; \
	then cd deps/raft && git pull origin master; \
	else git clone https://github.com/willemt/raft.git deps/raft; \
	fi
.PHONY : raft_fetch

raft: raft_fetch raft_build 
.PHONY : raft

libuv_build:
	cd deps/libuv && sh autogen.sh
	cd deps/libuv && ./configure
	cd deps/libuv && make
	mkdir -p include/uv
	mkdir -p libs/libuv
	cp -r deps/libuv/include/* ./include/uv/
	cp deps/libuv/.libs/libuv.a ./libs/libuv
.PHONY : libuv_build

libuv_fetch:
	if test -e deps/libuv; \
	then cd deps/libuv && git pull origin $(LIBUV_BRANCH); \
	else git clone https://github.com/libuv/libuv deps/libuv; \
	fi
	cd deps/libuv && git checkout $(LIBUV_BRANCH)
.PHONY : libuv_fetch

libuv: libuv_fetch libuv_build
.PHONY : libuv

deps: lsmt lmdb tpl raft_build libuv
.PHONY : deps

all: deps main
.PHONY : all

libuv_vendor:
	rm -rf deps/libuv/.git > /dev/null
.PHONY : libuv_vendor

clean_deps:
	rm -r deps/libuv
	rm -r deps/raft/build
	rm -r deps/tpl/build
	rm -r deps/lsmt/build
	rm -r deps/lmdb/build
.PHONY : clean_deps
