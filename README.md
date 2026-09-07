# Palantir Foundry Data Pipelines — From Fragmented Systems to a Single Source of Truth

**A Forward-Deployed Engineer's field guide to data integration with Foundry Pipeline Builder**

[![Platform](https://img.shields.io/badge/Platform-Palantir%20Foundry-1a1a2e?style=flat-square)](https://www.palantir.com/platforms/foundry/)
[![Tool](https://img.shields.io/badge/Tool-Pipeline%20Builder-0f6fde?style=flat-square)]()
[![Domain](https://img.shields.io/badge/Domain-Agnostic-6a1b9a?style=flat-square)]()
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)

> This repository is the first in a 4-part series that walks through a realistic Foundry engagement end-to-end.
> 📦 Part 1 — **Data Pipelines** (this repo) → 🧠 Part 2 — [Ontology](https://github.com/manuelbomi/palantir-foundry-ontology) → 🖥️ Part 3 — [Workshop Apps](https://github.com/manuelbomi/palantir-foundry-workshop-apps) → 🧭 Part 4 — [The FDE Playbook](https://github.com/manuelbomi/palantir-foundry-fde-playbook) (capstone)

---

## Table of Contents

1. [The Problem an FDE Walks Into](#the-problem-an-fde-walks-into)
2. [Why This Pattern Shows Up Everywhere](#why-this-pattern-shows-up-everywhere)
3. [What We're Building](#what-were-building)
4. [The Data](#the-data)
5. [Walkthrough: Building the Pipeline](#walkthrough-building-the-pipeline)
6. [Design Decisions an FDE Has to Defend](#design-decisions-an-fde-has-to-defend)
7. [Why Foundry, and Not Just Another ETL Tool](#why-foundry-and-not-just-another-etl-tool)
8. [How to Reuse This Pattern in Other Domains](#how-to-reuse-this-pattern-in-other-domains)
9. [Repo Contents](#repo-contents)
10. [Related Repositories](#related-repositories)

---

## The Problem an FDE Walks Into

Every Forward-Deployed Engineer (FDE) has lived some version of this story:

> A company just acquired a competitor. The IT unification project is a year away. In the meantime, two operational teams are running the *same business process* — taking and fulfilling customer orders — on two completely different systems, with two different schemas, two different customer ID formats, and no shared source of truth. Analysts are stitching spreadsheets together by hand. Metrics disagree depending on who you ask. Orders are falling through the cracks, and customers are noticing.

This scenario is deliberately generic, because it *is* generic. It is the shape of the very first problem an FDE is usually handed on a new account, regardless of industry:

- A **hospital network** that just merged two regional systems, each with its own patient-scheduling database
- A **manufacturer** consolidating supplier and inventory feeds after acquiring a competitor's plant
- A **bank** reconciling loan or transaction data across two core banking platforms post-merger
- A **logistics company** unifying shipment tracking from a legacy TMS and a newly acquired carrier's system
- A **government agency** merging casework or benefits data across two legacy case-management systems

The technology changes; the pattern does not: **two (or more) systems of record, one business process, no unified view.** The job is to stand up that unified view *fast*, without waiting a year for a "proper" data warehouse migration.

## Why This Pattern Shows Up Everywhere

FDEs are deployed on-site with customers specifically because this class of problem is rarely solved by installing software — it requires someone who can sit with the fulfillment manager, understand *why* the spreadsheets disagree, and translate that into a rigorous, reproducible pipeline. The technical skill this repo demonstrates — cleaning, joining, and unioning disparate operational datasets into one authoritative dataset — is the single most repeated exercise in a Foundry deployment, because nearly every new engagement starts with "our data lives in more than one place and nobody fully trusts it."

## What We're Building

Using **[Pipeline Builder](https://www.palantir.com/docs/foundry/pipeline-builder/overview)** — Foundry's low-code/no-code, Spark-backed transformation canvas — we take three raw operational datasets and produce a single, clean, deployed `all_orders` dataset that becomes the foundation for everything downstream (the Ontology in Part 2, and the operational app in Part 3).

```
orders_office_goods.csv ───┐
                            ├─► Clean ─► Join with customers ─┐
orders_bureau_txn.csv ─────┘                                  ├─► Union ─► all_orders (deployed dataset)
                                                               │
consolidated_customers.csv ───────────────────────────────────┘
```

## The Data

Three synthetic CSVs, standing in for two merging companies' order systems and their reconciled customer master:

| File | Represents | Rows | Key columns |
|---|---|---|---|
| `data/orders_office_goods.csv` | Legacy order system, Company A | 746 | `orderId`, `customer_id`, `dueDateTime`, `status`, `assignee` |
| `data/orders_bureau_transactional_system.csv` | Legacy order system, Company B (the acquired company) | 746 | `order_id`, `customer_id`, `order_due_date`, `status`, `assignee` |
| `data/consolidated_customers.csv` | Master customer crosswalk, already reconciled across both companies | 58 | `officegoods_customer_id`, `bureau_customer_id`, `consolidated_customer_id`, `customer_name` |

Notice the two order systems don't even agree on **column names** (`orderId` vs. `order_id`, `dueDateTime` vs. `order_due_date`) or **date formats** — a small but very real detail. This is exactly what makes the "just union two tables" instinct fail, and why cleaning has to happen first.

## Walkthrough: Building the Pipeline

### 1. Land in Foundry and scope the workspace

Every engagement starts with a project and folder structure that others on the team can navigate.

![Foundry landing page](images/01-welcome-page.png)

### 2. Open Pipeline Builder and bring in the raw sources

Pipeline Builder is created as a **Batch, Standard** pipeline — this use case needs neither the sub-second latency of a streaming pipeline nor the always-on compute of a high-frequency pipeline, so choosing the right pipeline class up front is itself a design decision an FDE makes deliberately, not by default.

![Using Pipeline Builder in Palantir Foundry](images/02-using-pipeline-builder-in-palantir-foundry.png)

### 3. Clean each source independently, before joining anything

Each raw dataset gets its own **Transform** node. For the acquired company's transactional data, the cleaning steps:

- Cast `order_due_date` from `Date` → `Timestamp` (so it can be compared/aggregated consistently downstream)
- Filter out rows where `order_id` is null or an empty string (protects the primary key)
- Normalize column names to a consistent convention

![Data Cleaning](images/03-data-cleaning.png)
![Data cleaning: isNotNull filter, drop columns, cast, rename](images/04-data-cleaning-isnotnull-filter-drop-columns-cast-rname-etc.png)

The legacy system undergoes the equivalent treatment, plus a rename (`dueDateTime` → `order_due_date`) so the two datasets can eventually share a schema.

> **FDE principle:** clean and standardize *before* you join or union. Trying to reconcile schema differences *during* a join creates pipelines that are unreadable six months later. Trying to do it *after* a union means you're debugging silent data quality issues in production.

### 4. Join each order dataset with the customer master

Each cleaned order dataset is joined to `consolidated_customers` on its respective customer-ID column, pulling in a shared `consolidated_customer_id` and `customer_name` — the fields that let the business finally talk about "the customer" instead of two different, incompatible ID systems.

![Inner Join of 2 tables](images/05-inner-join-of-2-tables.png)
![After Inner Join of 2 tables](images/06-after-inner-join-of-2-tables.png)
![Click to join 2 datasets](images/07-click-to-join-2-datasets.png)
![Another Join](images/08-another-join.png)
![After another join](images/09-after-another-join.png)

### 5. Union the two joined datasets into one

With both branches now sharing an identical schema, they can be safely **unioned** into a single logical table of orders — the moment the two companies' order books become one.

![Click to Union Join Bureau and Join Office Goods](images/10-click-to-union-join-bureau-and-join-office-goods.png)
![Union Orders](images/11-union-orders.png)
![After Union Orders](images/12-after-union-orders.png)
![Union Orders have 11 columns](images/13-union-orders-have-11-columns.png)
![After Union orders in the pipeline](images/14-after-union-orders-in-the-pipeline.png)

> **Common failure mode:** a union fails with a schema mismatch because one branch still calls the key `orderId` and the other calls it `order_id`. This isn't a bug in Foundry — it's the pipeline correctly refusing to silently merge incompatible schemas. The fix always lives upstream, in the cleaning transforms, not in the union step itself.

### 6. Materialize the result as a dataset other applications can consume

A pipeline's intermediate previews are *not* persisted outside the pipeline. To make the unified orders table usable by the Ontology (Part 2) and any other application, it must be explicitly written out as a **dataset output**.

![Add Output to the data set](images/15-add-output-to-the-data-set.png)
![Add output and click on New Dataset](images/16-add-output-and-click-on-new-dataset.png)
![Add output new dataset](images/17-add-output-new-dataset.png)
![Rename new_dataset_date to all_orders](images/18-rename-new-dataset-date-to-all-orders.png)

### 7. Deploy

Deploying compiles the visual pipeline into a scheduled, production-grade Spark job and writes `all_orders` to Foundry as a first-class dataset.

![Click on Save and click on Deploy](images/19-click-on-save-and-click-on-deploy.png)
![Click on deploy this pipeline](images/20-click-on-deploy-this-pipeline.png)
![Deploying the pipeline may take some time](images/21-deploying-the-pipeline-may-take-some-time.png)
![Pipeline successfully deployed](images/21b-pipeline-successfully-deployed.png)

At this point, `all_orders` is a real, governed, queryable Foundry dataset — ready to become the backbone of an Ontology object in [Part 2](https://github.com/manuelbomi/palantir-foundry-ontology).

## Design Decisions an FDE Has to Defend

A tutorial hides these; a real engagement does not. Questions a customer's architecture review will actually ask:

- **Why Batch/Standard and not streaming?** Order volume here doesn't require sub-minute latency; a streaming pipeline would add operational complexity (checkpointing, backpressure, always-on compute cost) with no corresponding business benefit. The right answer is "match the pipeline class to the freshness the business process actually needs," not "always pick the most powerful option."
- **Why clean before joining, not after?** Schema and type drift compounds. Cleaning per-source keeps each transform small, testable, and attributable to one upstream system — critical when something breaks and you need to know *which* source system changed.
- **Why keep all columns in the output?** In this case, downstream consumers (Ontology, Workshop) are still being defined, so it's cheaper to carry a wide table now and trim later than to re-run discovery on a truncated one. In a mature pipeline, you'd revisit this and drop what's genuinely unused.
- **What happens when a new order system gets added later (a third acquisition, a new region)?** Because cleaning is isolated per source and the join/union pattern is modular, adding a fourth branch is additive — a new clean → join → union edge — not a rewrite.

## Why Foundry, and Not Just Another ETL Tool

Palantir Foundry is used across manufacturing, healthcare, financial services, government, and defense specifically because it treats data integration as the *first* step of a longer chain — pipeline → Ontology → operational application — rather than a standalone ETL exercise whose output has to be re-integrated into some other BI or app layer:

- **Low-code where it helps, full-code where it matters.** Pipeline Builder is visual and fast for exactly the class of cleaning/join/union logic this project needed, but the same pipeline graph can drop into PySpark/Java transforms for anything more custom — no platform switch required.
- **Lineage and governance are automatic, not bolted on.** Every dataset in Foundry carries full lineage back to its raw sources, which matters enormously the first time a customer asks "where did this number come from?" mid-incident.
- **The output isn't the end state — it's an input.** `all_orders` isn't a report; it's a dataset that becomes an Ontology Object (Part 2) and powers a live operational app (Part 3). This is the core of Foundry's value proposition: one governed data layer serving pipelines, Ontology, apps, and AIP simultaneously, instead of the data being copied and re-modeled at every layer.

## How to Reuse This Pattern in Other Domains

The generic playbook, independent of industry:

1. **Identify the "same process, multiple systems" pain.** It's almost always there after a merger, acquisition, regional rollout, or legacy modernization effort.
2. **Profile each source independently** before attempting any join — column names, types, nullability, and primary key integrity always differ more than stakeholders expect.
3. **Standardize types and naming per-source**, so the join/union logic downstream is trivial and the pipeline stays debuggable.
4. **Pick the pipeline class deliberately** (batch vs. streaming, standard vs. high-frequency) based on the business's actual freshness requirement — not the most impressive option.
5. **Materialize a single, deployed, governed dataset** as the explicit hand-off point to the next layer of the platform (Ontology, an app, a model).

## Repo Contents

```
├── data/
│   ├── orders_office_goods.csv
│   ├── orders_bureau_transactional_system.csv
│   └── consolidated_customers.csv
├── images/                 # 21 annotated screenshots of the pipeline build, in build order
└── README.md
```

## Related Repositories

| Part | Repo | Focus |
|---|---|---|
| 1 | **palantir-foundry-data-pipelines** (this repo) | Data integration with Pipeline Builder |
| 2 | [palantir-foundry-ontology](https://github.com/manuelbomi/palantir-foundry-ontology) | Modeling the unified dataset as a live Ontology Object |
| 3 | [palantir-foundry-workshop-apps](https://github.com/manuelbomi/palantir-foundry-workshop-apps) | Turning the Ontology into an operational application |
| 4 | [palantir-foundry-fde-playbook](https://github.com/manuelbomi/palantir-foundry-fde-playbook) | The end-to-end case study and generalized FDE playbook |
| — | [palantir-foundry-lng-operations](https://github.com/manuelbomi/palantir-foundry-lng-operations) | Companion build: the same Workshop mechanics reframed for LNG cargo & terminal operations |

---

*Author: [manuelbomi](https://github.com/manuelbomi) — built while working through Palantir's official Foundry tutorial content, reframed around the kinds of problems a Forward-Deployed Engineer solves in the field.*
