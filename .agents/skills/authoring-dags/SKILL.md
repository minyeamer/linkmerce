---
name: authoring-dags
description: Workflow and best practices for writing Apache Airflow Dags. Use when the user wants to create a new Dag, write pipeline code, or asks about Dag patterns and conventions. For testing and debugging Dags, see the testing-dags skill.
hooks:
  Stop:
    - hooks:
        - type: command
          command: "echo 'Remember to test your Dag with the testing-dags skill'"
---

# Dag Authoring Skill

This skill guides you through creating and validating Airflow Dags using best practices and `af` CLI commands.

> **For testing and debugging Dags**, see the **testing-dags** skill which covers the full test -> debug -> fix -> retest workflow.

---

## Running the CLI

These commands assume `af` is on PATH. Run via `astro otto` to get it automatically, or install standalone with `uv tool install astro-airflow-mcp`.

---

## Workflow Overview

```
+-----------------------------------------+
| 1. DISCOVER                             |
|    Understand codebase & environment    |
+-----------------------------------------+
                 |
+-----------------------------------------+
| 2. PLAN                                 |
|    Propose structure, get approval      |
+-----------------------------------------+
                 |
+-----------------------------------------+
| 3. IMPLEMENT                            |
|    Write Dag following patterns         |
+-----------------------------------------+
                 |
+-----------------------------------------+
| 4. VALIDATE                             |
|    Check import errors, warnings        |
+-----------------------------------------+
                 |
+-----------------------------------------+
| 5. TEST (with user consent)             |
|    Trigger, monitor, check logs         |
+-----------------------------------------+
                 |
+-----------------------------------------+
| 6. ITERATE                              |
|    Fix issues, re-validate              |
+-----------------------------------------+
```

---

## Phase 1: Discover

Before writing code, understand the context.

### Explore the Codebase

Use file tools to find existing patterns:
- `Glob` for `**/dags/**/*.py` to find existing Dags
- `Read` similar Dags to understand conventions
- Check `requirements.txt` for available packages

### Query the Airflow Environment

Use `af` CLI commands to understand what's available:

| Command | Purpose |
|---------|---------|
| `af config connections` | What external systems are configured |
| `af config variables` | What configuration values exist |
| `af config providers` | What operator packages are installed |
| `af config version` | Version constraints and features |
| `af dags list` | Existing Dags and naming conventions |
| `af config pools` | Resource pools for concurrency |

**Example discovery questions:**
- "Is there a Snowflake connection?" -> `af config connections`
- "What Airflow version?" -> `af config version`
- "Are S3 operators available?" -> `af config providers`

---

## Phase 2: Plan

Based on discovery, propose:

1. **Dag structure** - Tasks, dependencies, schedule
2. **Operators to use** - Based on available providers
3. **Connections needed** - Existing or to be created
4. **Variables needed** - Existing or to be created
5. **Packages needed** - Additions to requirements.txt

**Get user approval before implementing.**

---

## Phase 3: Implement

Write the Dag following best practices (see below). Key steps:

1. Create Dag file in appropriate location
2. Update `requirements.txt` if needed
3. Save the file

---

## Phase 4: Validate

**Use `af` CLI as a feedback loop to validate your Dag.**

### Step 1: Check Import Errors

After saving, check for parse errors (Airflow will have already parsed the file):

```bash
af dags errors
```

- If your file appears -> **fix and retry**
- If no errors -> **continue**

Common causes: missing imports, syntax errors, missing packages.

### Step 2: Verify Dag Exists

```bash
af dags get <dag_id>
```

Check: Dag exists, schedule correct, tags set, paused status.

### Step 3: Check Warnings

```bash
af dags warnings
```

Look for deprecation warnings or configuration issues.

### Step 4: Explore Dag Structure

```bash
af dags explore <dag_id>
```

Returns in one call: metadata, tasks, dependencies, source code.

### On Astro

If you're running on Astro, you can also validate locally before deploying:

- **Parse check**: Run `astro dev parse` to catch import errors and Dag-level issues without starting a full Airflow environment
- **Dag-only deploy**: Once validated, use `astro deploy --dags` for fast Dag-only deploys that skip the Docker image build — ideal for iterating on Dag code

---

## Phase 5: Test

> See the **testing-dags** skill for comprehensive testing guidance.

Once validation passes, test the Dag using the workflow in the **testing-dags** skill:

1. **Get user consent** -- Always ask before triggering
2. **Trigger and wait** -- `af runs trigger-wait <dag_id> --timeout 300`
3. **Analyze results** -- Check success/failure status
4. **Debug if needed** -- `af runs diagnose <dag_id> <run_id>` and `af tasks logs <dag_id> <run_id> <task_id>`

### Quick Test (Minimal)

```bash
# Ask user first, then:
af runs trigger-wait <dag_id> --timeout 300
```

For the full test -> debug -> fix -> retest loop, see **testing-dags**.

---

## Phase 6: Iterate

If issues found:
1. Fix the code
2. Check for import errors: `af dags errors`
3. Re-validate (Phase 4)
4. Re-test using the **testing-dags** skill workflow (Phase 5)

---

## CLI Quick Reference

| Phase | Command | Purpose |
|-------|---------|---------|
| Discover | `af config connections` | Available connections |
| Discover | `af config variables` | Configuration values |
| Discover | `af config providers` | Installed operators |
| Discover | `af config version` | Version info |
| Validate | `af dags errors` | Parse errors (check first!) |
| Validate | `af dags get <dag_id>` | Verify Dag config |
| Validate | `af dags warnings` | Configuration warnings |
| Validate | `af dags explore <dag_id>` | Full Dag inspection |

> **Testing commands** -- See the **testing-dags** skill for `af runs trigger-wait`, `af runs diagnose`, `af tasks logs`, etc.

---

## Best Practices & Anti-Patterns

For code patterns and anti-patterns, see **[reference/best-practices.md](reference/best-practices.md)**.

**Read this reference when writing new Dags or reviewing existing ones.** It covers what patterns are correct (including Airflow 3-specific behavior) and what to avoid.

---

## Related Skills

- **testing-dags**: For testing Dags, debugging failures, and the test -> fix -> retest loop
- **debugging-dags**: For troubleshooting failed Dags
- **deploying-airflow**: For deploying Dags to production (Astro or open-source)
- **migrating-airflow-2-to-3**: For migrating Dags to Airflow 3
