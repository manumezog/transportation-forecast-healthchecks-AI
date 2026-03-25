"""
master.py — EU Logistics Forecast Health-Check Pipeline
========================================================

ORCHESTRATOR for the multi-agent health-check system. Runs automatically
every time a new forecast version is published and produces a self-contained
HTML executive dashboard summarising anomalies.

EXECUTION FLOW
--------------
  1. LOAD DATA         Read weekly_forecast_data.csv and recent_actuals.csv.
                       Auto-generates them via dummy_fcst_generator.py if not found.

  2. ANALYTICS         Pre-compute all statistics with pandas (zero API calls).
                       Two checks are performed:
                         • Variance Check  — v_current vs v_prior (week-on-week shift)
                         • Reality Check   — v_current forecast vs recent actuals (bias)

  3. AI NARRATIVE      Optionally call two Gemini agents to write executive prose
                       on top of the data. Skipped gracefully if API quota is
                       exhausted or no key is provided — the report is still full.

  4. HTML REPORT       Build and save logistics_report.html from pandas DataFrames.
                       Includes KPI bar, anomaly tables with severity badges
                       (CRITICAL / HIGH / OK), and actionable recommendations.

ANOMALY THRESHOLDS
------------------
  CRITICAL  |Δ| > 25%   — Immediate escalation required
  HIGH      |Δ| > 10%   — Flag for forecasting team review
  OK        |Δ| ≤ 10%   — Within acceptable bounds

Run:  python master.py
"""

import os, sys, time
from datetime import datetime
import pandas as pd

# ── .env loader ──────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# ── API setup (optional) ─────────────────────────────────────
_key = os.environ.get("GOOGLE_API_KEY", "")
AI_ENABLED = False
client = None

if _key:
    try:
        from google import genai
        from google.genai import types
        client = genai.Client(api_key=_key)
        AI_ENABLED = True
        print(f"🔑 API Key  : {_key[:4]}...{_key[-4:]} — AI narrative enabled")
    except Exception as e:
        print(f"⚠️  google-genai import failed ({e}). Running in data-only mode.")
else:
    print("ℹ️  No GOOGLE_API_KEY — running in data-only mode (full report still generated).")

MODEL = "gemini-2.0-flash"

# ============================================================
# 1. LOAD / AUTO-GENERATE DATA
# ============================================================
FORECAST_CSV = os.path.join(SCRIPT_DIR, "weekly_forecast_data.csv")
ACTUALS_CSV  = os.path.join(SCRIPT_DIR, "recent_actuals.csv")

if not os.path.exists(FORECAST_CSV) or not os.path.exists(ACTUALS_CSV):
    print("📦 Data files not found – auto-generating...")
    import dummy_fcst_generator
    dummy_fcst_generator.generate_massive_logistics_data()

print("📂 Loading data...")
df_f = pd.read_csv(FORECAST_CSV)
df_a = pd.read_csv(ACTUALS_CSV)
print(f"✅ {len(df_f):,} forecast rows | {len(df_a):,} actuals rows")

# ============================================================
# 2. PRE-COMPUTE ANALYTICS  (no API calls)
# ============================================================
print("\n📊 Pre-computing analytics...")

v_curr  = df_f[df_f["version"] == "v_current"]
v_prior = df_f[df_f["version"] == "v_prior"]

def pct(curr, prior):
    """
    Compute percentage change from prior to curr element-wise.

    Divides by prior, replacing zeros with NaN to avoid divide-by-zero.
    Returns values rounded to 1 decimal place (e.g. +12.3 means +12.3%).
    """
    return ((curr - prior) / prior.replace(0, float("nan")) * 100).round(1)

def dim_variance(col):
    """
    Aggregate total forecast qty for v_current and v_prior by a given dimension
    (e.g. 'country', 'lane_type', 'route') and compute the % change between them.

    Returns a DataFrame sorted by absolute % change descending so the
    highest-variance groups appear first.
    """
    c = v_curr.groupby(col)["qty"].sum()
    p = v_prior.groupby(col)["qty"].sum()
    df = pd.DataFrame({"curr_qty": c, "prior_qty": p, "change_pct": pct(c, p)})
    return df.sort_values("change_pct", key=abs, ascending=False)

country_var = dim_variance("country").head(10)
lane_var    = dim_variance("lane_type")
route_var   = dim_variance("route").head(10)

total_curr  = v_curr["qty"].sum()
total_prior = v_prior["qty"].sum()
total_pct_v = round((total_curr - total_prior) / total_prior * 100, 1)

# Reality check
fcst_agg   = v_curr.groupby("route")[["qty", "volume"]].sum()
actual_agg = df_a.groupby("route")[["actual_qty", "actual_volume"]].sum()
merged     = fcst_agg.join(actual_agg, how="inner")
merged["bias_pct"] = pct(merged["qty"], merged["actual_qty"])
merged["abs_bias"] = merged["bias_pct"].abs()

meta   = v_curr[["route", "country", "lane_type"]].drop_duplicates("route").set_index("route")
merged = merged.join(meta, how="left")

def dim_bias(col):
    """
    Aggregate forecasted vs actual qty by a given dimension and compute bias %.

    bias_pct = (forecast - actual) / actual × 100

    Positive bias → over-forecast (we predicted more than was shipped).
    Negative bias → under-forecast (we predicted less than was shipped).
    Sorted by absolute bias descending.
    """
    g = merged.groupby(col)[["qty", "actual_qty"]].sum()
    b = pct(g["qty"], g["actual_qty"])
    return pd.DataFrame({"fcst_qty": g["qty"], "actual_qty": g["actual_qty"], "bias_pct": b}).sort_values("bias_pct", key=abs, ascending=False)

country_bias = dim_bias("country").head(10)
lane_bias    = dim_bias("lane_type")
route_bias   = merged.nlargest(10, "abs_bias")[["country", "lane_type", "qty", "actual_qty", "bias_pct"]]

total_fcst   = merged["qty"].sum()
total_actual = merged["actual_qty"].sum()
total_bias   = round((total_fcst - total_actual) / total_actual * 100, 1)

n_var_anomalies  = int((country_var["change_pct"].abs() > 10).sum() + (lane_var["change_pct"].abs() > 10).sum())
n_bias_anomalies = int((country_bias["bias_pct"].abs() > 10).sum() + (lane_bias["bias_pct"].abs() > 10).sum())
worst_var  = float(country_var["change_pct"].abs().max())
worst_bias = float(country_bias["bias_pct"].abs().max())
total_routes = merged.shape[0]

print("✅ Analytics ready.")

# Text summaries for AI agents
variance_text = f"""Total packages — Current={total_curr:,.0f}, Prior={total_prior:,.0f}, Change={total_pct_v:+.1f}%

By Country (top 10):
{country_var.to_string()}

By Lane Type:
{lane_var.to_string()}

By Route (top 10):
{route_var.to_string()}"""

reality_text = f"""Total — Forecast={total_fcst:,.0f}, Actual={total_actual:,.0f}, Bias={total_bias:+.1f}%

By Country (top 10):
{country_bias.to_string()}

By Lane Type:
{lane_bias.to_string()}

Top 10 Routes by Bias:
{route_bias.to_string()}"""

# ============================================================
# 3. OPTIONAL AI NARRATIVE
# ============================================================
variance_narrative = ""
reality_narrative  = ""
ai_used = False

def call_agent(name, system_prompt, user_message, max_retries=2):
    """
    Send a single generate_content request to Gemini and return the response text.

    Each agent call represents one expert persona analysing pre-computed data:
      • VarianceAnalyst — writes narrative on version-over-version variance
      • RealityChecker  — writes narrative on forecast vs actuals bias

    Error handling strategy:
      - Quota exhausted (HTTP 429) → return empty string immediately.
        The report will still be generated from pandas data (no narrative block).
      - Other transient errors → retry up to max_retries times with a 5s pause.
      - All other errors → return empty string after retries (never crash).

    Parameters
    ----------
    name          : str   Label printed in the console (e.g. 'VarianceAnalyst')
    system_prompt : str   Expert persona and task instructions for the model
    user_message  : str   Pre-computed analytics text to include in the prompt
    max_retries   : int   Maximum number of attempts before giving up (default 2)

    Returns
    -------
    str  AI-generated narrative text, or empty string if unavailable.
    """
    for attempt in range(max_retries):
        try:
            resp = client.models.generate_content(
                model=MODEL,
                contents=user_message,
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt, temperature=0.2),
            )
            return resp.text
        except Exception as e:
            err = str(e)
            if "exhausted" in err.lower() or "429" in err or "quota" in err.lower():
                print(f"   ⚠️  {name}: quota exhausted — skipping AI (report still generated from data)")
                return ""   # ← bail immediately, no retries, no waiting
            else:
                print(f"   ⚠️  {name} error: {err[:100]}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                else:
                    return ""

if AI_ENABLED:
    print("\n🚀 Running AI agents (narrative enrichment)...\n" + "="*45)
    t_ai = time.time()
    try:
        print("🤖 [1/2] VarianceAnalyst...")
        variance_narrative = call_agent(
            "VarianceAnalyst",
            "Expert EU logistics Data Scientist. Analyse v-o-v variance statistics. Flag anomalies >10% as HIGH or CRITICAL (>25%). Provide concise executive narrative (3-5 sentences).",
            f"Analyse:\n{variance_text}"
        )
        print(f"   ✅ Done ({len(variance_narrative):,} chars)")

        print("🤖 [2/2] RealityChecker...")
        reality_narrative = call_agent(
            "RealityChecker",
            "Supply Chain Auditor. Analyse forecast vs actuals bias. Flag anomalies >10%. Identify systemic patterns. Concise executive narrative (3-5 sentences).",
            f"Analyse:\n{reality_text}"
        )
        print(f"   ✅ Done ({len(reality_narrative):,} chars)")
        ai_used = True
        print(f"✅ AI enrichment complete in {int(time.time()-t_ai)}s")
    except Exception as e:
        print(f"\n⚠️  AI unavailable after retries ({e.__class__.__name__}). Proceeding with data-only report.")
        print("   ℹ️  To fix: verify your API key has quota at aistudio.google.com > Settings > API\n")

# ============================================================
# 4. BUILD HTML REPORT  (always succeeds)
# ============================================================
print("\n📄 Generating HTML report...")

def severity_badge(val, threshold=10):
    """
    Return an inline HTML badge element colour-coded by anomaly severity.

    CRITICAL (red)   → |val| > 25%
    HIGH (orange)    → |val| > threshold (default 10%)
    OK (green)       → |val| ≤ threshold
    """
    if abs(val) > 25:
        return '<span class="ml-2 px-2 py-0.5 rounded text-xs font-bold bg-red-700 text-white">CRITICAL</span>'
    if abs(val) > threshold:
        return '<span class="ml-2 px-2 py-0.5 rounded text-xs font-bold bg-orange-600 text-white">HIGH</span>'
    return '<span class="ml-2 px-2 py-0.5 rounded text-xs bg-emerald-800 text-emerald-300">OK</span>'

def row_bg(val, threshold=10):
    """Return a Tailwind background class for a table row based on its anomaly severity."""
    if abs(val) > 25: return "bg-red-950 hover:bg-red-900"
    if abs(val) > threshold: return "bg-orange-950 hover:bg-orange-900"
    return "hover:bg-slate-700"

def fmt_num(v):
    """Format a number as a comma-separated integer string (e.g. 1234567 → '1,234,567')."""
    try: return f"{int(v):,}"
    except: return str(v)

def fmt_pct(v):
    """Format a percentage with sign and one decimal place (e.g. 12.3 → '+12.3%')."""
    try: return f"{float(v):+.1f}%"
    except: return str(v)

def variance_table_html(df, index_label="Group"):
    """
    Render a pandas DataFrame as a dark-themed HTML table for the variance section.

    Expects columns: curr_qty, prior_qty, change_pct (as produced by dim_variance).
    Rows are colour-coded by severity and include an inline badge.
    """
    rows = ""
    for idx, r in df.iterrows():
        pct_val = r.get("change_pct", 0)
        bg = row_bg(pct_val)
        rows += f"""
        <tr class="border-b border-slate-700/50 {bg}">
          <td class="px-4 py-3 font-medium">{idx}</td>
          <td class="px-4 py-3 text-right font-mono text-slate-300">{fmt_num(r.get('curr_qty', ''))}</td>
          <td class="px-4 py-3 text-right font-mono text-slate-300">{fmt_num(r.get('prior_qty', ''))}</td>
          <td class="px-4 py-3 text-right font-mono">{fmt_pct(pct_val)}{severity_badge(pct_val)}</td>
        </tr>"""
    return f"""<table class="w-full text-sm text-slate-300 text-left">
      <thead><tr class="text-xs text-slate-500 uppercase tracking-wider border-b border-slate-600">
        <th class="px-4 py-3">{index_label}</th>
        <th class="px-4 py-3 text-right">Current (pkgs)</th>
        <th class="px-4 py-3 text-right">Prior (pkgs)</th>
        <th class="px-4 py-3 text-right">Change</th>
      </tr></thead><tbody>{rows}</tbody></table>"""

def bias_table_html(df, index_label="Group"):
    """
    Render a pandas DataFrame as a dark-themed HTML table for the reality-check section.

    Expects columns: fcst_qty (or qty), actual_qty, bias_pct, and optionally
    country / lane_type for route-level subtitles (as produced by dim_bias or merged).
    Rows are colour-coded by severity and include an inline badge.
    """
    rows = ""
    for idx, r in df.iterrows():
        b = r.get("bias_pct", 0)
        bg = row_bg(b)
        country = r.get("country", "")
        lane = r.get("lane_type", "")
        meta = f' <span class="text-slate-500 text-xs">{country} · {lane}</span>' if country else ""
        rows += f"""
        <tr class="border-b border-slate-700/50 {bg}">
          <td class="px-4 py-3 font-medium">{idx}{meta}</td>
          <td class="px-4 py-3 text-right font-mono text-slate-300">{fmt_num(r.get('fcst_qty', r.get('qty', '')))}</td>
          <td class="px-4 py-3 text-right font-mono text-slate-300">{fmt_num(r.get('actual_qty', ''))}</td>
          <td class="px-4 py-3 text-right font-mono">{fmt_pct(b)}{severity_badge(b)}</td>
        </tr>"""
    return f"""<table class="w-full text-sm text-slate-300 text-left">
      <thead><tr class="text-xs text-slate-500 uppercase tracking-wider border-b border-slate-600">
        <th class="px-4 py-3">{index_label}</th>
        <th class="px-4 py-3 text-right">Forecast (pkgs)</th>
        <th class="px-4 py-3 text-right">Actual (pkgs)</th>
        <th class="px-4 py-3 text-right">Bias</th>
      </tr></thead><tbody>{rows}</tbody></table>"""

def kpi_card(title, value, subtitle="", color="indigo"):
    """
    Return an HTML KPI metric card for the top summary bar.

    Parameters: title (label), value (large number), subtitle (small note),
    color (Tailwind colour prefix: 'indigo', 'orange', 'red', etc.).
    """
    return f"""<div class="bg-slate-800 rounded-xl p-5 border border-slate-700">
      <p class="text-xs uppercase tracking-wider text-slate-500 mb-1">{title}</p>
      <p class="text-3xl font-bold text-{color}-400">{value}</p>
      {f'<p class="text-xs text-slate-500 mt-1">{subtitle}</p>' if subtitle else ""}
    </div>"""

def section(title, icon, narrative, table_html):
    """
    Wrap a report section in a dark card with optional AI narrative block.

    If narrative is non-empty (AI was available), it is displayed as an
    indigo-bordered callout above the data table.
    If narrative is empty (data-only mode), the callout is omitted.
    """
    narrative_block = f"""<div class="mb-6 p-4 bg-slate-700/40 rounded-lg border-l-4 border-indigo-500">
      <p class="text-slate-300 leading-relaxed">{narrative}</p>
    </div>""" if narrative else ""
    return f"""<div class="bg-slate-800 rounded-2xl p-6 border border-slate-700 mb-6">
      <h2 class="text-xl font-semibold text-white mb-4 flex items-center gap-2">{icon} {title}</h2>
      {narrative_block}
      <div class="overflow-x-auto rounded-lg border border-slate-700">{table_html}</div>
    </div>"""

ts = datetime.now().strftime("%Y-%m-%d %H:%M UTC+1")
ai_badge = (
    '<span class="text-xs bg-indigo-700 text-indigo-200 px-2 py-1 rounded-full">✨ AI Enhanced</span>'
    if ai_used else
    '<span class="text-xs bg-slate-700 text-slate-400 px-2 py-1 rounded-full">📊 Data-Driven</span>'
)

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>EU Logistics Forecast Health-Check</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    body {{ font-family: 'Inter', system-ui, sans-serif; }}
    @keyframes fadeIn {{ from {{ opacity:0; transform:translateY(8px) }} to {{ opacity:1; transform:translateY(0) }} }}
    .fade-in {{ animation: fadeIn 0.4s ease-out forwards; }}
  </style>
</head>
<body class="bg-slate-900 text-slate-200 min-h-screen">

  <!-- Header -->
  <div class="bg-gradient-to-r from-slate-900 via-indigo-950 to-slate-900 border-b border-slate-700">
    <div class="max-w-7xl mx-auto px-6 py-8">
      <div class="flex items-center justify-between flex-wrap gap-4">
        <div>
          <p class="text-indigo-400 text-sm font-medium uppercase tracking-widest mb-1">Amazon EU Transportation · Forecast Intelligence</p>
          <h1 class="text-3xl font-bold text-white">EU Logistics Forecast Health-Check</h1>
          <p class="text-slate-400 text-sm mt-2">Generated {ts} · {total_routes:,} routes analysed · {ai_badge}</p>
        </div>
        <div class="text-right">
          <p class="text-slate-500 text-xs">Week-on-Week Comparison</p>
          <p class="text-indigo-300 font-mono text-lg">v_current vs v_prior</p>
        </div>
      </div>
    </div>
  </div>

  <div class="max-w-7xl mx-auto px-6 py-8 space-y-6 fade-in">

    <!-- KPI Bar -->
    <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
      {kpi_card("Routes Analysed", f"{total_routes:,}", "Joined forecast × actuals")}
      {kpi_card("Variance Anomalies", str(n_var_anomalies), "> 10% volume shift", "orange")}
      {kpi_card("Worst Variance", f"{worst_var:+.1f}%", "Max country-level change", "red" if worst_var > 25 else "orange")}
      {kpi_card("Worst Actuals Bias", f"{worst_bias:+.1f}%", "Max country-level gap", "red" if worst_bias > 25 else "orange")}
    </div>

    <!-- Overall Summary -->
    <div class="bg-slate-800 rounded-2xl p-6 border border-slate-700 grid md:grid-cols-2 gap-6">
      <div>
        <p class="text-xs text-slate-500 uppercase tracking-wider mb-1">Overall Forecast Volume Change</p>
        <p class="text-4xl font-bold {'text-red-400' if abs(total_pct_v) > 25 else 'text-orange-400' if abs(total_pct_v) > 10 else 'text-emerald-400'}">{total_pct_v:+.1f}%</p>
        <p class="text-slate-400 text-sm mt-1">{total_curr:,.0f} pkgs (current) vs {total_prior:,.0f} (prior)</p>
      </div>
      <div>
        <p class="text-xs text-slate-500 uppercase tracking-wider mb-1">Overall Forecast vs Actuals Bias</p>
        <p class="text-4xl font-bold {'text-red-400' if abs(total_bias) > 25 else 'text-orange-400' if abs(total_bias) > 10 else 'text-emerald-400'}">{total_bias:+.1f}%</p>
        <p class="text-slate-400 text-sm mt-1">{total_fcst:,.0f} pkgs forecast vs {total_actual:,.0f} actual</p>
      </div>
    </div>

    <!-- Section 1: Version-over-Version Variance -->
    {section(
        "Version-over-Version Variance", "📈",
        variance_narrative,
        "<h3 class='text-slate-400 text-xs uppercase px-4 py-2'>By Country</h3>" +
        variance_table_html(country_var, "Country") +
        "<h3 class='text-slate-400 text-xs uppercase px-4 py-2 mt-4'>By Lane Type</h3>" +
        variance_table_html(lane_var, "Lane Type") +
        "<h3 class='text-slate-400 text-xs uppercase px-4 py-2 mt-4'>Top 10 Routes by Variance</h3>" +
        variance_table_html(route_var, "Route")
    )}

    <!-- Section 2: Reality Check -->
    {section(
        "Forecast vs Actuals Reality Check", "🎯",
        reality_narrative,
        "<h3 class='text-slate-400 text-xs uppercase px-4 py-2'>By Country</h3>" +
        bias_table_html(country_bias, "Country") +
        "<h3 class='text-slate-400 text-xs uppercase px-4 py-2 mt-4'>By Lane Type</h3>" +
        bias_table_html(lane_bias, "Lane Type") +
        "<h3 class='text-slate-400 text-xs uppercase px-4 py-2 mt-4'>Top 10 Routes by Bias</h3>" +
        bias_table_html(route_bias.set_index("route") if "route" in route_bias.columns else route_bias, "Route")
    )}

    <!-- Section 3: Recommendations -->
    <div class="bg-slate-800 rounded-2xl p-6 border border-slate-700">
      <h2 class="text-xl font-semibold text-white mb-4">💡 Actionable Recommendations</h2>
      <ol class="space-y-3 text-slate-300">
        <li class="flex gap-3"><span class="text-indigo-400 font-bold">1.</span> Investigate all CRITICAL (&gt;25% variance) country/lane combinations immediately — escalate to the relevant regional forecasting team.</li>
        <li class="flex gap-3"><span class="text-indigo-400 font-bold">2.</span> For HIGH bias routes (&gt;10% vs actuals), review recent demand drivers (promotions, seasonality shifts, network changes).</li>
        <li class="flex gap-3"><span class="text-indigo-400 font-bold">3.</span> If Air lanes are consistently over-forecast, consider applying a systematic lift/scale correction before next publish.</li>
        <li class="flex gap-3"><span class="text-indigo-400 font-bold">4.</span> Validate that the v_current generation pipeline did not have data issues — large overall swings may indicate upstream data problems.</li>
        <li class="flex gap-3"><span class="text-indigo-400 font-bold">5.</span> Schedule a forecast review call with subregional planners for any country with &gt;20% change to align on root-cause.</li>
      </ol>
    </div>

  </div>

  <!-- Footer -->
  <div class="border-t border-slate-800 mt-8 py-6 text-center text-slate-600 text-xs">
    Generated by AI Forecast Health-Check System · Amazon EU Transportation Forecasting
  </div>

</body>
</html>"""

# ============================================================
# 5. SAVE REPORT
# ============================================================
out_path = os.path.join(SCRIPT_DIR, "logistics_report.html")
with open(out_path, "w", encoding="utf-8") as f:
    f.write(html)

print(f"\n🎉 Report saved to: {out_path}")
print("   Open it in your browser!")