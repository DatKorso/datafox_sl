# Repository Guidelines

## Project Structure & Module Organization
- `app.py` — Streamlit entry point.
- `pages/` — Streamlit pages (numbered prefix pattern, e.g. `12_🚨_Проблемы_Карточек_OZ.py`).
- `utils/` — reusable modules (e.g., `db_connection.py`, `cross_marketplace_linker.py`).
- `tests/` — pytest suite (top-level feature tests and `tests/unit/`).
- `data/` — local inputs/outputs used during analysis.
- `project-docs/` — documentation (`technical/`, `user-guides/`).
- `.streamlit/` — Streamlit configuration.

## Build, Test, and Development Commands
- Install (uv): `uv sync` (uses `.venv` from `pyproject.toml`/`uv.lock`).
- Run app: `uv run streamlit run app.py`.
- Run tests: `uv run pytest -q` (filter: `pytest -k name -q`).
- Format (optional): if you use Black/ruff locally, run before PR; no enforced config in repo.

## Coding Style & Naming Conventions
- Python 3.10+, 4-space indentation, PEP 8.
- Names: `snake_case` for modules/functions/variables; `PascalCase` for classes.
- Streamlit pages: keep numbered prefix + short descriptive title.
- Prefer type hints in new/updated `utils/` code; keep UI logic in `pages/`, business logic in `utils/`.

## Testing Guidelines
- Framework: pytest with fixtures in `tests/conftest.py`.
- Structure: unit tests in `tests/unit/`; feature/integration tests at `tests/test_*.py`.
- Naming: files `test_*.py`, functions `test_*`.
- Expectations: add/adjust tests for new logic and edge cases; run `pytest -q` locally before PR.

## Commit & Pull Request Guidelines
- Commit style: Conventional Commits (e.g., `feat: ...`, `fix: ...`, `refactor: ...`).
- Scope: keep messages concise; English or Russian accepted; reference issues when applicable.
- PRs: include summary of changes, rationale, screenshots for UI changes, and links to docs updated under `project-docs/`.
- Checklist: tests pass, app runs (`streamlit run app.py`), configs not leaking secrets.

## Security & Configuration Tips
- Copy example config: `cp config.example.json config.json`; avoid committing secrets.
- Use `.streamlit/secrets.toml` for sensitive values when needed.
- Never commit real data dumps to VCS; prefer small, anonymized samples in `data/`.

