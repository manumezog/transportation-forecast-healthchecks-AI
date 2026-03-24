# 🚢 Transportation Forecast Automated AI Health-Checks: Multi-Agent Analysis

An automated supply chain audit system powered by **Google Agent Development Kit (ADK)** and **Gemini 3 Flash**. This project uses a team of autonomous AI agents to conduct weekly "health checks" on logistics transportation forecasts across European routes.

---

## 📖 The Origin Story

**Hello!** I'm currently a **Forecasting Manager in EU Transportation at Amazon**, where my team is responsible for producing high-quality demand forecasts for transportation and labor capacity planning across the European Union on a weekly basis.

When managing thousands of routes every single week, ensuring that the forecasts remain accurate and don't deviate significantly from recent reality or prior versions is a colossal, highly complex task. Manual auditing is practically impossible at this scale. 

**This repository is a weekend side-hustle project I built for fun in my spare time.** It serves as an open-source prototype of a solution I came up with to solve this very real-world problem. While I am actively working internally to deploy a similar fully-scaled architecture at my day job, this open-source version implements the exact core logic and AI agent workflows using generated *dummy data*, demonstrating how Multi-Agent LLMs can autonomously perform massive-scale data health audits.

---

## 📌 Overview

In large-scale logistics, maintaining the integrity of capacity forecasts is critical. This system automates the forecast audit by deploying specialized AI agents that:

1. **Analyze Version-over-Version Variance:** Detects massive spikes or drops between forecast iterations to prevent capacity whiplash.
2. **Validate against Reality:** Compares past forecasts to actual shipment data to identify systemic bias or drift.
3. **Generate Executive Reports:** Synthesizes technical data anomalies into a stakeholder-ready HTML/Tailwind CSS dashboard.

## 🛠️ Tech Stack

- **Orchestration:** Google ADK (Agent Development Kit)
- **Brain:** Gemini 3 Flash (via Vertex AI / Google AI Studio)
- **Data Handling:** Python, Pandas, SQLAlchemy (In-Memory SQLite)
- **Reporting:** HTML5, Tailwind CSS

## 🕵️ The Agent Team

| Agent                  | Role           | Responsibility                                                     |
| :--------------------- | :------------- | :----------------------------------------------------------------- |
| **Variance Analyst**   | Data Scientist | Queries SQL to find major shifts between 'v_prior' and 'v_current'.|
| **Reality Checker**    | Auditor        | Joins forecast tables with actuals to find accuracy gaps >10%.     |
| **Logistics Reporter** | UI/UX Designer | Consolidates findings into a color-coded Tailwind HTML report.     |

## 🚀 Getting Started

### 1. Prerequisites

- Python 3.10+
- A Google Gemini API Key (Gemini Pro/Flash subscription)
- Required Libraries: 
  ```bash
  pip install google-adk pandas sqlalchemy
  ```

### 2. Setup Data

Run the data generator to create the "rigged" dummy datasets (`weekly_forecast_data.csv` and `recent_actuals.csv`) used to simulate the European transportation network:

```bash
python dummy_fcst_generator.py
```

### 3. Run the Pipeline

Execute the multi-agent orchestration script to start the autonomous audit:

```bash
python master.py
```

### 4. View Results

Open `logistics_report.html` in any web browser to see the synthesized anomalies detected and presented by the `Logistics Reporter` agent.

---

## 🧠 Key Learning: Agentic Data Analysis

This project implements the **"Tool-Use" pattern**. Instead of passing raw CSV data to the LLM (which often causes hallucinations or token limits), the agents are given a highly-constrained SQL Query Tool. The agents intuitively "think" about what data they need, write a SQL query to aggregate it, and receive a calculated summary back. This pattern ensures **100% mathematical accuracy** while leveraging the LLM's reasoning and summarization capabilities.
