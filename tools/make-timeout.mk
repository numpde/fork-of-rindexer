# Keep one shell for the whole recipe so a deadline covers every command and
# its cleanup. Recursive Make calls stay inside their caller's remaining budget.
rindexer_make_tools := $(dir $(lastword $(MAKEFILE_LIST)))
MAKE_TIMEOUT ?= 30m
export MAKE_TIMEOUT
SHELL := /bin/bash
.SHELLFLAGS = $(rindexer_make_tools)make-recipe.sh -c
.ONESHELL:
