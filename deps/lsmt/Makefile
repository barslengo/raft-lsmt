# Compiler
CC = gcc
AR = ar

# Compiler flags
CFLAGS_DEBUG = -W -Wall -g -I./include -DDEBUG
CFLAGS_RELEASE = -W -Wall -O3 -flto -s -I./include -DRELEASE

ifeq ($(BUILD_TYPE), debug)
    CFLAGS = $(CFLAGS_DEBUG)
else
    CFLAGS = $(CFLAGS_RELEASE)
endif

# Linker flags - Note: -L and -l flags are now used for linking the executable
LDFLAGS = -lm -lpthread 

# Source files
SRC = $(wildcard src/*.c)
TEST_SRC = $(wildcard test/*.c)

# Separate main.c from other source files which will form the library
MAIN_SRC = src/main.c
APP_SRC = $(filter-out $(MAIN_SRC), $(SRC))

# Object files for the application/library
APP_OBJ = $(patsubst %.c, build/%.o, $(APP_SRC))

# Object file for the main entry point
MAIN_OBJ = build/src/main.o

# Test object files
TEST_OBJ = $(patsubst test/%.c, build/tests/%.o, $(TEST_SRC))

# Executable targets
EXEC = build/lsmt
TEST_EXEC = build/tests/test_runner

# --- Static Library Configuration ---
# Directory to store the static library
STATIC_LIB_DIR = build/lib
# Full path to the static library file (convention is lib<name>.a)
STATIC_LIB = $(STATIC_LIB_DIR)/liblsmt.a

# Default target: build the main executable
all: $(EXEC)

# New Target: build only the static library
static: $(STATIC_LIB)

# Rule to create the static library from the application object files
$(STATIC_LIB): $(APP_OBJ)
	@echo "Creating static library $@"
	@mkdir -p $(dir $@)
	$(AR) rcs $@ $^

# --- Linking Targets ---

# Link main executable against the static library
$(EXEC): $(MAIN_OBJ) $(STATIC_LIB)
	@echo "Linking executable $@"
	$(CC) -o $@ $(MAIN_OBJ) -L$(STATIC_LIB_DIR) -llsmt $(LDFLAGS)

# Link test executable: app objects + test objects (no main.o or library)
$(TEST_EXEC): $(APP_OBJ) $(TEST_OBJ)
	@echo "Linking test runner $@"
	$(CC) -o $@ $^ $(LDFLAGS)

# --- Compilation Rules ---

# Compile source files to object files
build/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

# Compile test source files to object files
build/tests/%.o: test/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@


# --- Utility Targets ---

clean:
	rm -rf build/*

debug: BUILD_TYPE=debug
debug: all

release: BUILD_TYPE=release
release: all

# Run tests
test: BUILD_TYPE=release
test: $(TEST_EXEC)
	@echo "Running tests..."
	./$(TEST_EXEC)

# Add 'static' to the list of phony targets
.PHONY: all clean debug release test static