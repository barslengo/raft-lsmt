# --- Configuration ---

# Name of the static library (e.g., 'mylib' becomes 'libmylib.a')
LIB_NAME = raft

# Compiler and Archiver
CC = gcc
AR = ar

# Directories
SRC_DIR   = .
BUILD_DIR = build
OBJ_DIR   = $(BUILD_DIR)/obj
LIB_DIR   = $(BUILD_DIR)/lib

# Compiler flags (-I adds the 'include' directory to the search path)
CFLAGS = -Wall -Wextra -O3 

# --- Build Definitions ---

# Find all .c source files in the source directory
SOURCES = $(wildcard $(SRC_DIR)/*.c)

# Create a list of object file paths in the object directory
OBJECTS = $(patsubst $(SRC_DIR)/%.c, $(OBJ_DIR)/%.o, $(SOURCES))

# The full path to the final static library file
STATIC_LIB = $(LIB_DIR)/lib$(LIB_NAME).a

# --- Build Rules ---

# Default target: build the static library.
all: $(STATIC_LIB)

# Rule to create the static library from all object files.
$(STATIC_LIB): $(OBJECTS)
	@echo "AR  Creating static library $@"
	@mkdir -p $(LIB_DIR)
	$(AR) rcs $@ $^

# Pattern rule to compile any .c file into its corresponding .o file.
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
	@echo "CC  Compiling $<"
	@mkdir -p $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

# Rule to clean up all build artifacts.
clean:
	@echo "CLEAN Removing build directory"
	@rm -rf $(BUILD_DIR)

# Declare targets that are not files.
.PHONY: all clean
