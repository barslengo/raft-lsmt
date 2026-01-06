LIBUV_BRANCH=v1.51.0
RAFT_TAG=main #v0.22.1

# Compiler
CC = gcc

# --- Libraries (Common Linker Flags) ---
# We define libraries separately so we can reuse them in both Debug and Release
LIBS = -L./libs/lsmt -llsmt \
       -L./libs/libuv -luv \
       -L./libs/raft -lraft \
       -lpthread -lm -llz4

# --- Configuration Sets ---

# Debug: ASAN, Symbols, No Optimization, Frame Pointers (for better stack traces)
# Note: -fsanitize=address must be passed to BOTH compiler AND linker
CFLAGS_DEBUG = -W -Wall -fsanitize=address -fno-omit-frame-pointer -g -I./include -I./include/uv -DDEBUG
LDFLAGS_DEBUG = -fsanitize=address $(LIBS)

# Release: Optimization, Linux specific defines
CFLAGS_RELEASE = -W -Wall -O3 -I./include -I./include/uv -DRELEASE -DLINUX
LDFLAGS_RELEASE = $(LIBS)

# --- Default Variables (Can be overridden by targets) ---
# Default to Release mode if just 'make' is run
CFLAGS = $(CFLAGS_RELEASE)
LDFLAGS = $(LDFLAGS_RELEASE)

# --- Files ---
SRC = $(wildcard src/*.c)
INCLUDED_SRC = src/main.c 

# Separate main.c from other source files which will form the library
MAIN_SRC = src/server.c
APP_SRC = $(filter-out $(MAIN_SRC) $(INCLUDED_SRC), $(SRC))

# Object files mapping (e.g., src/file.c -> build/src/file.o)
APP_OBJ = $(patsubst %.c, build/%.o, $(APP_SRC))
MAIN_OBJ = build/src/server.o

# Executable targets
EXEC = build/server

# --- Main Targets ---

# Default target
all: release

# Link main executable
# Uses whatever CFLAGS/LDFLAGS are currently set
$(EXEC): $(MAIN_OBJ) $(APP_OBJ) 
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

# Compile source files to object files
build/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

# --- Build Modes ---

# Target-specific variable overrides. 
# When 'make debug' is run, these variables replace the defaults for the scope of the rule.

debug: CFLAGS = $(CFLAGS_DEBUG)
debug: LDFLAGS = $(LDFLAGS_DEBUG)
debug: $(EXEC)
	@echo "Build complete: DEBUG Mode (ASAN Enabled)"

release: CFLAGS = $(CFLAGS_RELEASE)
release: LDFLAGS = $(LDFLAGS_RELEASE)
release: $(EXEC)
	@echo "Build complete: RELEASE Mode"

# --- Utility Targets ---

clean:
	rm -rf build/*

lsmt:
	cd deps/lsmt && make static
	mkdir -p include/lsmt
	mkdir -p libs/lsmt
	cp deps/lsmt/include/*.h ./include/lsmt/ 
	cp deps/lsmt/build/lib/liblsmt.a ./libs/lsmt
.PHONY : lsmt

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

raft_build:
	cd deps/raft && autoreconf -i
	cd deps/raft && ./configure \
		UV_CFLAGS="-I$(CURDIR)/deps/libuv/include" \
		UV_LIBS="-L$(CURDIR)/deps/libuv/.libs -L$(CURDIR)/deps/libuv -luv"
	cd deps/raft && make
	mkdir -p include/raft
	mkdir -p libs/raft
	cp -r deps/raft/include/* ./include/raft/
	cp deps/raft/.libs/libraft.a ./libs/raft
.PHONY : raft_build

raft_fetch:
	if test -e deps/raft; \
	then cd deps/raft && git pull origin main; \
	else git clone https://github.com/cowsql/raft deps/raft; \
	fi
	cd deps/raft && git checkout $(RAFT_TAG)
.PHONY : raft_fetch

raft: raft_fetch raft_build 
.PHONY : raft

deps: lsmt libuv raft
.PHONY : deps

libuv_vendor:
	rm -rf deps/libuv/.git > /dev/null
.PHONY : libuv_vendor

clean_deps:
	rm -r deps/lsmt/build
	rm -r deps/libuv/build
	rm -r deps/raft/build
.PHONY : clean_deps

.PHONY : main debug release clean all
