.PHONY: check

# One locked local entry point; the optional hosted workflow delegates here.
check:
	uv run --locked --extra dev ruff check .
	uv run --locked --extra dev pytest
