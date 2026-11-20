# Upload Guide for ocean-incremental-sync

This folder contains everything the platform needs. The files are numbered in the order in which
they are used, and the steps below explain each one.

## Step 0 - Propose the repo

Before uploading anything else, the repo has to be proposed and approved by a lead. The following
text can be pasted into the proposal form:

> **Repo:** port-labs/ocean (https://github.com/port-labs/ocean), written in Python.
>
> **Base commit:** 902a7c5e670268d612527b857d901c715e2d6da3, dated November 19, 2026.
>
> **About the project:** Ocean is Port's open-source framework for building data integrations,
> which pull information from tools such as GitHub, Jira or Datadog into Port's developer portal.
> It is a production tool under the Apache-2.0 license and receives several commits every day.
>
> **Size and tests:** the repo has about 320,000 non-blank lines of Python, of which the framework
> itself makes up roughly 50,000. Its 1,500 framework tests use pytest, need no network access and
> finish in about 70 seconds.
>
> **Planned task:** integration tests for the incremental resync feature.

Please also ask the lead to confirm that this repo is not on the excluded list (SWE-Bench Pro and
SWE-Atlas Test Writing). We could not check those lists ourselves.

## Step 1 - The repo snapshot

The snapshot is the base commit above with `1_snapshot_test_removal.patch` applied. The command
`git apply 1_snapshot_test_removal.patch` does this. The patch removes 42 existing tests from the
folder `port_ocean/tests/core/incremental/` and changes nothing else.

## Step 2 - The instruction

Copy the text of `2_instruction.md` into the instruction field.

## Step 3 - The gold tests

The folder `3_gold_tests/` contains three test files. They belong at these paths inside the repo:

- `port_ocean/tests/core/incremental/test_incremental_resync_flow.py`
- `port_ocean/tests/core/incremental/test_pagination_strategies.py`
- `port_ocean/tests/clients/port/mixins/test_integration_cursors.py`

If the platform asks for a command to run them, this one works:

```
pytest -o addopts='' -p no:cacheprovider port_ocean/tests/core/incremental/test_incremental_resync_flow.py port_ocean/tests/core/incremental/test_pagination_strategies.py port_ocean/tests/clients/port/mixins/test_integration_cursors.py
```

## Step 4 - The planted bugs (27 mutants)

The bugs come in two formats, and you can choose whichever the platform accepts. The folder
`4_mutants/patches/` holds one patch file per bug, from `M01.patch` to `M27.patch`, made against the
base commit. The folder `4_mutants/mutated_files/` holds the same bugs as complete source files,
with one subfolder per bug.

Each bug needs a scenario tag and a difficulty tag. The file `4_mutants/mutant_tags.csv` lists both
for every bug, and the summary below shows the same information.

The easy bugs are M01, M03, M04, M08, M16, M21 and M22. Ten bugs are medium: M02, M05, M09, M10,
M12, M13, M17, M23, M24 and M25. The remaining ten are hard, namely M06, M07, M11, M14, M15, M18,
M19, M20, M26 and M27.

By scenario, M01 to M07 belong to S1 (cursor progression across runs) and M08 to M11 to S2 (failed
kinds). S3 (multiple mapping blocks) covers M12 to M15, while S4 (storing cursors in Port) covers
M16 to M20. The last group, M21 to M27, belongs to S5 (fetching only what changed).

## Step 5 - The metadata

The values come from `5_metadata.json`. The language is Python, and the repo is port-labs/ocean at
commit `902a7c5e6`. The test level is integration, and the tests use pytest with pytest-asyncio. The
features under test are listed in the field `functionalities_under_test`. For the difficulty, the
task aims at a top-model score of about 60, although our own runs suggest it may land higher.

## Step 6 - Platform checks

Run the platform's automatic checks. They verify the format and the offline build, and they confirm
that the gold tests score 100 while an empty test set scores 0. If the gold score is not 100, look at
the environment first. Python must be 3.12 or newer, the test packages must be installed, and smoke
tests must be excluded. On our computer the gold tests passed three runs in a row.

## Step 7 - The Opus 5.5 run and the tuning note

Start the top-model run. Once its score is known, write the tuning note. The text below can be used
as a starting point; only the platform score needs to be filled in.

> We tuned the task in three local rounds. In each round a fresh Opus 5.5 agent received only the
> ticket and the snapshot. The first version scored 100 because the ticket listed too many edge
> cases. We shortened the ticket, removed three bugs that it had practically announced, and added
> five harder ones that we had tested in advance. In the final local runs, the agents scored 85.2 and
> 88.9. They had unlimited time and wrote 73 to 87 tests each. The tests originally written by the
> repo's own engineers scored 33.3. The official platform run scored __. The bugs that the agents
> still missed involve rare setups, such as general full-resync handlers, requests without a login
> header, services that already return results newest-first, and dates in other time zones.

If the official score is above 70 or below 50, chapter 07 of the Documents folder
(`07_Step6_Calibration.md`) explains the next tuning steps.
