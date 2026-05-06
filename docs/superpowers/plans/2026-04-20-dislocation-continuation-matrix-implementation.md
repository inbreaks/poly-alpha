# Pulse Dislocation Continuation Matrix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a minimal `15m` continuation matrix that keeps `structure_core` as the trigger, executes on the same-timestamp complementary claim, and produces a reproducible checkpoint/report across the two existing research days.

**Architecture:** Keep everything inside `scripts/pulse_research_sweep.py`. Stream the full feature files, extract only `structure_core` trigger rows with paired complementary execution rows, compute execution-side `15m` outcomes from raw tape side series, and rank a hard-coded continuation strategy matrix.

**Tech Stack:** Python 3, `pandas`, `unittest`, existing zstd/jsonl helpers, existing checkpoint writer

---

### Task 1: Add failing continuation tests

**Files:**
- Modify: `scripts/tests/test_pulse_research_sweep.py`
- Modify: `scripts/pulse_research_sweep.py`

- [ ] **Step 1: Write failing tests for continuation pairing, execution-side exit derivation, and checkpoint output**
- [ ] **Step 2: Run the targeted tests and confirm the new cases fail for the expected missing helpers**
- [ ] **Step 3: Implement the minimal helpers required by those tests**
- [ ] **Step 4: Re-run the targeted tests and confirm they pass**

### Task 2: Implement continuation candidate extraction and ranking

**Files:**
- Modify: `scripts/pulse_research_sweep.py`
- Modify: `scripts/tests/test_pulse_research_sweep.py`

- [ ] **Step 1: Add a streaming extractor that keeps only `structure_core` trigger rows from full feature files**
- [ ] **Step 2: Pair each trigger row with the same-timestamp complementary execution row**
- [ ] **Step 3: Build execution-side `15m` outcomes from raw tape side series**
- [ ] **Step 4: Rank the hard-coded continuation strategy matrix with the existing cooldown and checkpoint style**

### Task 3: Add a runnable entry point and produce research outputs

**Files:**
- Modify: `scripts/pulse_research_sweep.py`
- Modify: `data/reports/pulse_research/dislocation_matrix_20260420/` output files

- [ ] **Step 1: Add a CLI stage for the continuation matrix with labeled feature-path and tape-dir inputs**
- [ ] **Step 2: Run the two-day continuation sweep on `20260418` and `20260420_partial_retry`**
- [ ] **Step 3: Save one short markdown summary next to the checkpoints**
- [ ] **Step 4: Re-run the full unit test file before reporting conclusions**
