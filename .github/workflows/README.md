# OpenBB Workflows

This directory contains the workflows for the OpenBB 🦋 Project. The workflows are:

## 📑 Deploy to GitHub Pages

This GitHub Actions workflow is responsible for building the documentation and deploying it to GitHub Pages. This workflow is triggered when a new change is pushed to the `main` or `release` branch of the repository, and the documentation is published to GitHub Pages.

## Branch Name Check

Objective: To check if pull request branch names follow the GitFlow naming convention before merging.

Triggered by: A pull request event where the target branch is either develop or main.

Branches checked: The source branch of a pull request and the target branch of a pull request.

Steps:

1. Extract branch names: Using the jq tool, the source and target branch names are extracted from the pull request event. The branch names are then stored in environment variables and printed as output.

2. Show Output result for source-branch and target-branch: The source and target branch names are printed to the console.

3. Check branch name for develop PRs: If the target branch is develop, then the source branch is checked against a regular expression to ensure that it follows the GitFlow naming convention. If the branch name is invalid, a message is printed to the console and the workflow exits with a status code of 1.

4. Check branch name for main PRs: If the target branch is main, then the source branch is checked against a regular expression to ensure that it is either a hotfix or a release branch. If the branch name is invalid, a message is printed to the console and the workflow exits with a status code of 1.

Note: The GitFlow naming convention for branches is as follows:

- Feature branches: feature/<feature-name>
- Hotfix branches: hotfix/<hotfix-name>
- Release branches: release/<major.minor.patch>(rc<number>)

## Deploy to PyPI - Nightly

This workflow is used to publish the latest version of the OpenBB Platform CLI to PyPI. The workflow is triggered at UTC+0 daily by the GitHub Action schedule event.

It does this by first updating the `pyproject.toml` file with a pre-determined version string of the form `<currentVersion>.dev<date>`, where `<date>` represents the current day's date as a 8 digit number.

Then, the code installs `pypa/build` and uses `python -m build` to create a binary wheel and a source tarball in the `dist/` directory.

Finally, it uses the PyPA specific action `gh-action-pypi-publish` to publish the created files to PyPI.

## Deploy the OpenBB Platform to Test PyPI

The Github Action code `Deploy to PyPI` is used to deploy a Python project to PyPI (Python Package Index) and TestPyPI, which is a separate package index for testing purposes. The code is triggered on two events:

1. Push event: The code is triggered whenever there is a push to the `release/*` and `main` branches.

2. Workflow dispatch event: The code can be manually triggered by the workflow dispatch event.

The code sets the concurrency to the `group` and the option `cancel-in-progress` is set to `true` to ensure that the running jobs in the same `group` are cancelled in case another job is triggered.

The code contains two jobs, `deploy-test-pypi` and `deploy-pypi`, both of which have the same steps with slight variations.

The `deploy-test-pypi` job is triggered only if the pushed branch starts with `refs/heads/release/`. This job sets up the Python environment, installs the `build` package using `pip`, builds binary wheel and source tarball using `build`, and finally, publishes the distributions to TestPyPI using the `pypa/gh-action-pypi-publish@release/v1` Github Action. The `password` to access TestPyPI is stored as a secret named `TEST_PYPI_API_TOKEN`.

Similarly, the `deploy-pypi` job is triggered only if the pushed branch starts with `refs/heads/main`. This job follows the same steps as `deploy-test-pypi`, but the distributions are published to PyPI instead of TestPyPI. The `password` to access PyPI is stored as a secret named `PYPI_API_TOKEN`.

Note: The code uses the `pypa/build` package for building the binary wheel and source tarball, and the `pypa/gh-action-pypi-publish@release/v1` Github Action for publishing the distributions to PyPI and TestPyPI.

## Draft release

This GitHub Actions workflow is designed to automatically generate and update draft releases in a GitHub repository. The workflow is triggered when it is manually dispatched, allowing you to control when the draft releases are updated.

## 🧹 General Linting

This GitHub Actions workflow runs linting checks on pull request events (`opened`, `synchronize`, and `edited`) and merge queue candidates. It uses GitHub Actions caching to improve performance.

It consists of Python/spelling, Markdown, and JSON/YAML validation jobs.

The first job, `code-linting`, runs on an Ubuntu machine and performs several linting tasks on the code in the repository, including:

- Checking out the merge candidate with complete Git history
- Setting up Python 3.12.13 and installing the local lab inventory and explicit tools
- Checking repository spelling with codespell
- Checking changed production Python with Black, mypy and pylint
- Checking changed Python, including tests and CI scripts, with Ruff without autofixes
- Retaining every tool's result and failing when any check fails

The second job, `markdown-link-check`, runs on an Ubuntu machine and performs linting of the markdown files in the repository. It uses a Docker container `avtodev/markdown-lint` to perform the linting.

## 🏷️ Pull Request Labels

Automatic labelling of pull requests.

## 🚉 Integration test Platform (API)

Run `openbb_platform` API integration tests,

## 🖥️ Unit test CLI

Run `cli` directory unit tests.

## 🚉 Unit test Platform

Run the explicit internal lab inventory described below.

## Internal lab CI

The Platform job uses **CPython 3.12.13 on Ubuntu 24.04**, with one interpreter and
no version matrix. This is a dependency choice, independent of any application
venv: the internal dqlib release is `dqlib-3.0.2-cp312-cp312-linux_x86_64.whl`
(see `quantitative/openbb_quantitative/_dqlib.py`), Qlib 0.9.7 publishes a Linux
CPython 3.12 wheel, and the collector/research/provider requirements intersect at
3.12. Python 3.12 receives security fixes through October 2028. Version 3.12.13
is a stable patch available to both setup-python and the isolated local runner.
Sources: [Python lifecycle](https://devguide.python.org/versions/),
[Python 3.12.13](https://www.python.org/downloads/release/python-31213/),
[Qlib release files](https://pypi.org/project/pyqlib/0.9.7/#files).

`.github/lab-ci.toml` is the installation and test inventory. Pip resolves all
listed local distributions together, followed by `pip check`. The upstream
`openbb` meta-package and unrelated upstream providers are not installed. FMP is
included because existing core registry/query fixtures require it; yfinance,
IBKR, AKShare, BaoStock, DQ, DuckDB and Stooq support the lab's market workflows.
The reference, Polars, Alphalens, Qlib and bt adapters and their actual libraries
are installed; preflight imports make missing optional libraries a failure.

| Checkout | Required offline coverage |
| --- | --- |
| develop (`fd11cf873` at repair baseline) | Platform metadata, core, shared provider/router checks, alpha/research with real Qlib/bt/Polars/Alphalens, quantitative/dqlib bridge, Platform API, MCP, collection, all three collectors, listed provider suites, charting/technical/econometrics, CFFEX cutover and installed synthetic CLI |
| PR6 (`7dcdf5bdd` at audit) | All baseline suites plus Futu, CTP native SDK and the live collector's tick tests; CTP gets a generated GB18030 locale and `CTP_REQUIRE_SDK=1` |

Futu and CTP are absent from current develop because PR6 is unmerged. The driver
prints this absence explicitly. When both package trees exist, it installs and
tests them automatically; a partial pair fails. No production tick code is
copied into this independent CI PR.

Provider declarations describe registry model inventory and test-suite ownership.
They support parameterized tests and the research providers' shared alpha suite;
new standard models require an inventory update, and every declared suite must
pass in the current attempt. They do not claim per-fetcher dynamic coverage.
Traditional providers retain the existing fetcher-test checks.

The router inventory keeps canonical paths, including their slash hierarchy, and
checks actual generated Python callables and API schemas. Its declared offline
suites must also pass in the current attempt. Legacy integration fixture
inspection uses globally installed providers, so a case naming an installed but
unsupported provider still fails. The explicit `unavailable_routes` inventory
identifies 15 legacy upstream commands whose models are absent from the lab;
each exception requires its source file checksum, function and model to match,
and ceases to apply when the model is available. This is inspection scope for
uninstalled upstream functionality, not a pytest skip or live-service validation.
Collection and dqlib command examples are documented in their actual router
metadata; licensed dqlib examples are not executed without its runtime.

Tests run sequentially in separate pytest processes to isolate repeated module
names, registry state and collector fixture mutations. Clean-checkout metadata
tests run before `openbb-build`; interface tests run after the build, including
`OPENBB_DUCKDB_INTERFACE_TEST=1`. Private settings use the existing
`scripts/alpha_research/sitecustomize.py`; application settings and venvs are not
used. Install the Ubuntu `bubblewrap` and `locales` packages first. The test stage
requires an isolated filesystem and a network namespace with only loopback, including
all subprocesses and native code. Synthetic local HTTP/WebSocket tests remain
available. Bubblewrap supplies a private user-owned filesystem root, an empty private home,
loopback networking and zero effective capabilities. Real directory ownership and
permission checks remain active, including Futu trusted-ancestor checks. No live account or external market service is contacted.

On the hosted Ubuntu 24.04 runner, the job loads the named
`.github/scripts/lab-bwrap.apparmor` profile and selects it explicitly with
`--apparmor-profile openbb-lab-ci-bwrap`. This permits user namespaces for the
sandbox launcher under Ubuntu's AppArmor restrictions; it has no executable
attachment and does not change global sysctls. The first suite verifies the
actual sandbox's ownership, loopback-only networking and zero effective
capabilities. See [Ubuntu's per-application user namespace policy](https://ubuntu.com/blog/ubuntu-23-10-restricted-unprivileged-user-namespaces).
Local machines without this restriction can omit the profile option. A denied
namespace or failed profile setup fails the job; there is no unsandboxed fallback.

Each attempt retains JUnit, exact integration deselections, skip reasons, exit
codes and counts. Collection errors, missing reports, empty/all-skipped suites,
failed builds and failed installs fail the job. Existing credentialed integration
markers are excluded by `-m 'not integration'`; the legacy integration-marked
auto-build reload smoke and seven synthetic econometrics functions (14 cases)
are explicitly run after the build through `offline_integration_tests`, using
exact file/node selections. Live docstring examples and econometrics cases that
fetch market data remain outside offline execution. DQ's offline
`test_integration.py` and alpha's real research-engine tests still run. Existing licensed dqlib native
tests remain collected and report their gate as skips when the proprietary wheel
and runtime license are unavailable. Provision that runtime and set
`OPENBB_RUN_DQLIB_NATIVE_TESTS=1` to validate it; the ordinary bridge tests run
without it. Other pre-existing skips are retained with their exact reasons.

To reproduce, use a fresh disposable source copy because the clean-checkout tests
intentionally reject previously generated assets. Create a new Python 3.12.13
venv and activate it, then run from that copy:

```bash
python .github/scripts/lab_ci.py install
python .github/scripts/run_lab_offline.py
python .github/scripts/lint_ci.py --base origin/develop
```

The lint driver compares against the actual merge base with full Git history.
Black, mypy and pylint check changed production Python; Ruff also checks changed
tests and CI scripts with `--no-fix`. Every linter runs and any failure propagates.
Mypy uses the installed editable source roots with explicit package bases, so
different local distributions resolve the same imported package consistently.
Explicit tool versions live in `.github/requirements-lab-ci.txt`; resolved package
versions and the selected inventory are uploaded with each run. Legacy CLI and
desktop workflows are outside this Platform lab job.

## Local provider fixture checks

The development installer includes DuckDB, Stooq and DQ Quant Data; the latter
retains its Python 3.11 through 3.13 requirement. The lab inventory installs these
packages explicitly and remains the source of the lab CI test selection.

EIA and CFTC fetcher tests replay saved HTTP cassettes without live credentials.
Response header scrubbing replaces existing values with a list containing
`MOCK_VALUE` and leaves absent headers absent. Invalid null header entries have
been removed from the saved cassettes. Run each provider suite separately:

```bash
python -m pytest openbb_platform/providers/eia/tests
python -m pytest openbb_platform/providers/cftc/tests
python -m pytest openbb_platform/providers/ibkr/tests
```

The IBKR suite supplies an event loop before collection when the interpreter has
none, including Python 3.14, and closes only the loop it created. Its tests use
local mocks and do not require TWS or IB Gateway.
