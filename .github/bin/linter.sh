#!/usr/bin/env bash
set -eu

main() {
    export TOP_DIR=$(git rev-parse --show-toplevel)

    # Format Python files
    black "." --exclude 'databricks/'

    # Lint SQL
    sqlfluff fix -f "." --dialect postgres
    # sqlfluff fix -f "${TOP_DIR}"/transform/dbt/adventureworks

    # If the linter produce diffs, fail the linter
    if [ -z "$(git status --porcelain)" ]; then 
        echo "Working directory clean, linting passed"
        exit 0
    else
        echo "Linting failed. Please commit these changes:"
        git --no-pager diff HEAD
        exit 1
    fi

}

main
