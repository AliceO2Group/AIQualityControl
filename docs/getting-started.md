# uv

This project uses **[uv](https://github.com/astral-sh/uv)** instead of the classic `pip` + `venv` + `requirements.txt` workflow.

---

## What is `uv`?

[`uv`](https://astral.sh/uv) is a **modern, ultra-fast Python package and environment manager** written in Rust.  
It replaces a whole toolbox (`pip`, `venv`, `pyenv`, `pip-tools`, `poetry`, etc.) with **one clean CLI**.

It is like:  

> `pip` + `venv` + `pyenv` + `pip-tools` + `poetry` = **`uv`**

---

## ⚡ Why We Use `uv` Instead of `pip`

### 1. Speed
`uv` installs packages **in parallel**, uses an **on-disk cache**, and is written in **Rust**, so installations and dependency resolution are dramatically faster than `pip`.

| Task | pip | uv |
|------|-----|----|
| Install packages | Serial | Parallel |
| Dependency resolver | Slow | Rust-optimized |
| Reinstall same deps | Redownload | Cached & instant |

---

### 2. Reproducible Environments
`uv` uses a **lockfile** (`uv.lock`) to pin exact versions of every dependency (like `poetry.lock` or `package-lock.json`).

Everyone — you, your teammates, and CI — get *identical* environments automatically:

```bash
uv sync
# 🧠 Using `uv` in This Project

This project uses **[uv](https://github.com/astral-sh/uv)** instead of the classic `pip` + `venv` + `requirements.txt` workflow.

---

## 🚀 What is `uv`?

[`uv`](https://astral.sh/uv) is a **modern, ultra-fast Python package and environment manager** written in Rust.  
It replaces a whole toolbox (`pip`, `venv`, `pyenv`, `pip-tools`, `poetry`, etc.) with **one clean CLI**.

You can think of it as:  

> `pip` + `venv` + `pyenv` + `pip-tools` + `poetry` = **`uv`**

---

## ⚡ Why We Use `uv` Instead of `pip`

### 1. ⚡ Speed
`uv` installs packages **in parallel**, uses an **on-disk cache**, and is written in **Rust**, so installations and dependency resolution are dramatically faster than `pip`.

| Task | pip | uv |
|------|-----|----|
| Install packages | Serial | Parallel |
| Dependency resolver | Slow | Rust-optimized |
| Reinstall same deps | Redownload | Cached & instant |

---

### 2. 🔒 Reproducible Environments
`uv` uses a **lockfile** (`uv.lock`) to pin exact versions of every dependency (like `poetry.lock` or `package-lock.json`).

Everyone — you, your teammates, and CI — get *identical* environments automatically:

```bash
uv sync
```

With `pip`, this reproducibility usually requires maintaining `requirements.txt` manually.

---

### 3. 🧠 No More “Activate” Dance
You never need to run `source .venv/bin/activate`.  
`uv` automatically runs commands inside the right virtual environment:

```bash
uv run python script.py
uv run pytest
uv run mkdocs serve
```

It just works — no manual steps.

---

### 4. 🐍 Full Python Version Control
`uv` also manages **Python itself**.  
You can install and pin the exact version your project needs:

```bash
uv python install 3.12
uv python pin 3.12
```

That ensures everyone (and CI) uses the same interpreter version.

---

### 5. 🧩 Cleaner, Modern Configuration
All dependencies and metadata live in a single file: **`pyproject.toml`**.  
No more `requirements.txt` or `setup.py`.

Adding or removing dependencies updates this file automatically:

```bash
uv add requests
uv add --dev pytest
```

This is the modern Python standard (PEP 621 compliant).

---

### 6. 🔁 Backward Compatible
`uv` understands **pip syntax** and can behave like pip if you need it:

```bash
uv pip install somepackage
uv pip freeze
```

So you’re not locked in — it’s fully compatible with the old ecosystem.

---

## 🧩 Contributor Workflow

Here’s the standard flow when working on this project.

---

### 1. 🛠 Install `uv`

```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows (PowerShell)
powershell -ExecutionPolicy Bypass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

---

### 2. 📦 Set up your environment

```bash
# clone the repo
git clone https://github.com/<user>/<repo>.git
cd <repo>

# (optional) install & pin Python version
uv python install 3.12
uv python pin 3.12

# create virtualenv + install deps
uv sync
```

---

### 3. ▶️ Run commands

```bash
# run your script
uv run python your_script.py

# run tests
uv run pytest -q

# lint / format
uv run ruff check .
uv run ruff format .

# run docs locally
uv run mkdocs serve
```

---

### 4. ➕ Add dependencies

```bash
# add runtime dependency
uv add requests

# add dev-only dependency
uv add --dev pytest ruff mkdocs mkdocs-material 'mkdocstrings[python]'
```

Both `pyproject.toml` and `uv.lock` will update — **commit them both**.

---

### 5. 🔄 Update environment

```bash
# upgrade to latest compatible versions
uv lock --upgrade
# install updated packages
uv sync
```

---

### 6. 📚 Build docs for deployment

```bash
uv run mkdocs build
# static site will be generated in ./site/
```

---

## 🧱 Quick Reference

| Task | Command |
|------|----------|
| Install deps | `uv sync` |
| Run tests | `uv run pytest` |
| Lint code | `uv run ruff check .` |
| Serve docs | `uv run mkdocs serve` |
| Add dependency | `uv add <pkg>` |
| Add dev dependency | `uv add --dev <pkg>` |
| Update all deps | `uv lock --upgrade && uv sync` |
| Pin Python version | `uv python pin 3.12` |

---

## 💡 Summary

**Why we use `uv`:**

✅ Much faster than pip  
✅ Manages environments automatically  
✅ Guarantees reproducible builds with `uv.lock`  
✅ No need to activate venvs manually  
✅ Modern `pyproject.toml` workflow  
✅ Handles Python versions, dependencies, and builds in one tool  
✅ Compatible with pip when needed  

---

> `uv` is the new standard for modern, reliable, and efficient Python development.  
> It makes contributing easier, faster, and safer.

---
