# 🗺️ Brownfield Cartographer

> **Multi-agent codebase intelligence system for rapid FDE onboarding in production environments.**
> Point it at any GitHub repo or local path. Get a living, queryable map of the system's architecture, data flows, and semantic structure in under 60 seconds.

---

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/Meseretbolled/brownfield-cartographer.git
cd brownfield-cartographer

# 2. Create virtual environment and install
uv sync
source .venv/bin/activate

# 3. Run analysis on any repo
cartographer analyze /path/to/repo

# 4. Run analysis on a GitHub URL (auto-clones)
cartographer analyze https://github.com/dbt-labs/jaffle_shop

# 5. Launch interactive query interface
cartographer query /path/to/repo

# 6. Print summary of existing analysis
cartographer summary /path/to/repo
```

---

## Verify It Works

Run these commands to confirm everything is working end-to-end:

```bash
# 1. Check the CLI is installed and responds
cartographer --help

# 2. Run against the included jaffle_shop artefacts (no cloning needed)
cartographer query . --cartography-dir cartography-artifacts/jaffle_shop

# 3. Inside the query interface, try:
navigator> sources
navigator> sinks
navigator> blast_radius orders
navigator> module schema
navigator> quit

# 4. Run a fresh analysis against jaffle_shop
git clone https://github.com/dbt-labs/jaffle_shop /tmp/jaffle_shop
cartographer analyze /tmp/jaffle_shop

# 5. Inspect generated artefacts
ls /tmp/jaffle_shop/.cartography/
cat /tmp/jaffle_shop/.cartography/analysis_summary.md
```

---

## What It Does

The Cartographer runs four agents in sequence against any codebase:

| Agent | Role | Output |
| --- | --- | --- |
| **Surveyor** | Static AST analysis — module graph, PageRank, git velocity, dead code | `module_graph.json` |
| **Hydrologist** | Data lineage — Python dataflow, SQL (sqlglot), YAML/DAG configs, notebooks | `lineage_graph.json` |
| **Semanticist** | LLM purpose statements, doc drift detection, domain clustering, Day-One answers | `semanticist_trace.json` |
| **Archivist** | Produces all final artefacts — CODEBASE.md, onboarding brief, audit log | `CODEBASE.md`, `onboarding_brief.md` |

The **Navigator** agent provides an interactive query interface over the generated knowledge graph.

---

## Commands

### `analyze` — Full pipeline

```bash
cartographer analyze <repo>

# Options:
#   --output, -o        Custom output directory (default: <repo>/.cartography/)
#   --incremental, -i   Only re-analyse files changed since last run
#   --git-days          Days of git history for velocity (default: 30)

# Examples:
cartographer analyze /tmp/jaffle_shop
cartographer analyze https://github.com/dbt-labs/jaffle_shop
cartographer analyze /tmp/jaffle_shop --output ./my-output --git-days 60
cartographer analyze /tmp/jaffle_shop --incremental
```

### `query` — Interactive Navigator

```bash
cartographer query <repo>

# Inside the navigator:
blast_radius <node>          # All downstream dependents
lineage <dataset>            # Upstream sources of a dataset
module <path>                # Full detail on a module
sources                      # All data ingestion entry points
sinks                        # All data output endpoints
hubs                         # Top modules by PageRank
quit                         # Exit
```

### `summary` — Quick summary

```bash
cartographer summary <repo>
```

---

## Generated Artefacts

Every analysis run produces these files in `.cartography/`:

| File | Description |
| --- | --- |
| `module_graph.json` | Full module import graph with PageRank scores |
| `lineage_graph.json` | Data lineage DAG (datasets + transformations) |
| `analysis_summary.md` | Human-readable run summary |
| `CODEBASE.md` | Living context file — inject into any AI coding agent |
| `onboarding_brief.md` | Five FDE Day-One questions answered with evidence |
| `cartography_trace.jsonl` | Audit log of every agent action |

---

## Architecture

```mermaid
flowchart TD
    INPUT["📁 Repo Input\n(local path or GitHub URL)"]

    subgraph CORE["Core Infrastructure"]
        MODELS["src/models/__init__.py\nPydantic Schemas\nModuleNode · DatasetNode\nTransformationNode · Edge"]
        KG["src/graph/knowledge_graph.py\nKnowledgeGraph\nNetworkX DiGraph\nPageRank · BFS · SCC"]
    end

    subgraph ANALYZERS["Language Analyzers"]
        TSA["src/analyzers/tree_sitter_analyzer.py\nMulti-language AST Parser\nPython · JS · YAML"]
        SQL["src/analyzers/sql_lineage.py\nsqlglot SQL Parser\ndbt ref() · CTEs · JOINs"]
        DAG["src/analyzers/dag_config_parser.py\nYAML/Config Parser\nAirflow DAGs · dbt schema.yml"]
    end

    subgraph AGENTS["Analysis Agents"]
        SUR["🔭 Surveyor\nsrc/agents/surveyor.py\nModule graph · PageRank\nGit velocity · Dead code"]
        HYD["💧 Hydrologist\nsrc/agents/hydrologist.py\nData lineage DAG\nblast_radius · sources/sinks"]
        SEM["🧠 Semanticist\nsrc/agents/semanticist.py\nLLM purpose statements\nDoc drift · Domain clusters\nDay-One answers"]
        ARC["🗄️ Archivist\nsrc/agents/archivist.py\nCODEBASE.md\nonboarding_brief.md\ncartography_trace.jsonl"]
    end

    subgraph QUERY["Query Interface"]
        NAV["🧭 Navigator\nsrc/agents/navigator.py\nfind_implementation()\ntrace_lineage()\nblast_radius()\nexplain_module()"]
    end

    subgraph OUTPUTS["Generated Artefacts"]
        MG["module_graph.json"]
        LG["lineage_graph.json"]
        CM["CODEBASE.md"]
        OB["onboarding_brief.md"]
        TR["cartography_trace.jsonl"]
    end

    CLI["src/cli.py\ncartographer analyze\ncartographer query\ncartographer summary"]
    ORCH["src/orchestrator.py\nPipeline orchestration\nIncremental mode\nError isolation"]

    INPUT --> CLI
    CLI --> ORCH
    ORCH --> SUR
    ORCH --> HYD
    TSA --> SUR
    SQL --> HYD
    DAG --> HYD
    SUR --> KG
    HYD --> KG
    KG --> MODELS
    SUR --> ARC
    HYD --> ARC
    KG --> SEM
    SEM --> ARC
    ARC --> CM
    ARC --> OB
    ARC --> TR
    SUR --> MG
    HYD --> LG
    KG --> NAV
    NAV --> QUERY

    style CORE fill:#1e3a5f,color:#fff
    style ANALYZERS fill:#1a4731,color:#fff
    style AGENTS fill:#4a1942,color:#fff
    style QUERY fill:#4a3000,color:#fff
    style OUTPUTS fill:#3a1a00,color:#fff
```

---

## Project Structure

```
brownfield-cartographer/
├── src/
│   ├── cli.py                          # Entry point: analyze, query, summary
│   ├── orchestrator.py                 # Pipeline wiring + incremental mode
│   ├── models/__init__.py              # Pydantic schemas (all node/edge types)
│   ├── graph/knowledge_graph.py        # NetworkX wrapper + serialization
│   ├── analyzers/
│   │   ├── tree_sitter_analyzer.py     # Multi-language AST parsing
│   │   ├── sql_lineage.py              # sqlglot SQL dependency extraction
│   │   └── dag_config_parser.py        # Airflow/dbt YAML config parsing
│   └── agents/
│       ├── surveyor.py                 # Module graph, PageRank, git velocity
│       ├── hydrologist.py              # Data lineage graph
│       ├── semanticist.py              # LLM purpose statements, doc drift
│       ├── archivist.py                # CODEBASE.md, onboarding brief
│       └── navigator.py               # Interactive query agent
├── cartography-artifacts/
│   └── jaffle_shop/                    # Pre-generated artefacts (jaffle_shop)
│       ├── module_graph.json
│       ├── lineage_graph.json
│       └── analysis_summary.md
├── pyproject.toml
└── README.md
```

---

## Supported Languages & Patterns

| Language | What's Extracted |
| --- | --- |
| **Python** | Imports, functions, classes, pandas/PySpark/SQLAlchemy dataflow |
| **SQL / dbt** | Table dependencies, CTEs, JOINs, `ref()` calls |
| **YAML** | Airflow DAG topology, dbt `schema.yml` sources and models |
| **Jupyter** | `.ipynb` cell source — read/write data references |
| **JavaScript/TypeScript** | AST parsing (imports, exports) |

---

## Environment Variables

```bash
# LLM model selection (Semanticist agent)
ANTHROPIC_API_KEY=sk-ant-...          # Required for LLM features
CARTOGRAPHER_FAST_MODEL=claude-haiku-4-5-20251001   # Bulk summaries
CARTOGRAPHER_STRONG_MODEL=claude-haiku-4-5-20251001 # Synthesis tasks
CARTOGRAPHER_DOMAIN_K=6               # Number of domain clusters
```

> LLM features are **optional**. All static analysis (Surveyor + Hydrologist) works without any API key.

---

## Target Codebases Tested

| Repo | Modules | Datasets | Transformations |
| --- | --- | --- | --- |
| [dbt jaffle_shop](https://github.com/dbt-labs/jaffle_shop) | 3 | 9 | 5 |

---

## Dependencies

Key dependencies (see `pyproject.toml` for full list):

- `tree-sitter` — multi-language AST parsing
- `sqlglot` — SQL parsing and lineage extraction
- `networkx` — graph construction, PageRank, BFS
- `pydantic` — schema validation
- `typer` + `rich` — CLI and terminal output
- `gitpython` — git history analysis
- `anthropic` — LLM calls (optional)
