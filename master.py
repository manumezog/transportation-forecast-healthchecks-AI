print("DEBUG: master.py is starting...")
import os
import pandas as pd
from sqlalchemy import create_engine
from google.adk.agents import LlmAgent, ParallelAgent, SequentialAgent
try:
    from google.adk.runners import Runner as AgentRunner
except ImportError:
    try:
        from google.adk.runtime import AgentRunner
    except ImportError:
        from google.adk.agents import Agent as AgentRunner  # Fallback to generic Agent if needed
try:
    from google.adk.tools import ToolContext, FunctionTool
except ImportError:
    try:
        from google.adk.tools.tool_context import ToolContext
        from google.adk.tools.function_tool import FunctionTool
    except ImportError:
        # Fallback if FunctionTool is not found
        class FunctionTool:
            def __init__(self, func): self.func = func
        try:
            from google.adk.tools.tool_context import ToolContext
        except ImportError:
            ToolContext = any # Mock for now

# ==========================================
# 1. SETUP IN-MEMORY DATABASE
# ==========================================
# Auto-generate data if missing
script_dir = os.path.dirname(os.path.abspath(__file__))
forecast_path = os.path.join(script_dir, "weekly_forecast_data.csv")
actuals_path = os.path.join(script_dir, "recent_actuals.csv")

if not os.path.exists(forecast_path) or not os.path.exists(actuals_path):
    print("Dummy datasets not found. Auto-generating them now...")
    import dummy_fcst_generator
    dummy_fcst_generator.generate_logistics_data()

df_forecasts = pd.read_csv(forecast_path)
df_actuals = pd.read_csv(actuals_path)

memory_engine = create_engine('sqlite:///:memory:')
df_forecasts.to_sql('forecasts', memory_engine, index=False)
df_actuals.to_sql('actuals', memory_engine, index=False)

# ==========================================
# 2. DEFINE THE BULLETPROOF TOOL
# ==========================================
def query_logistics_data(query: str, tool_context: ToolContext) -> str:
    """Executes a SQL query on the logistics datasets."""
    safe_query = query
    if "limit" not in query.lower():
        safe_query = f"{query} LIMIT 100" 

    try:
        db_engine = tool_context.state["db_engine"]
        result = pd.read_sql(safe_query, db_engine)
        
        if len(result) >= 100:
            return result.to_string() + "\n\nWARNING: Max rows reached. Please use GROUP BY."
        return result.to_string()
        
    except Exception as e:
        schema_reminder = """
        Valid 'forecasts' cols: version, route, date, qty, volume, country, lane_type
        Valid 'actuals' cols: route, date, actual_qty, actual_volume
        """
        return f"SQL Error: {e}. \nSchema reminder: {schema_reminder}. Fix your query."

# ==========================================
# 3. DEFINE THE AGENTS
# ==========================================
variance_agent = LlmAgent(
    name="VarianceAnalyst",
    model="gemini-2.0-pro-exp",
    tools=[FunctionTool(func=query_logistics_data)],
    output_key="variance_anomalies",
    instruction="""You are an expert Data Scientist specializing in Variance Analysis. Your goal is to compare the new transportation package forecast ('v_current') against the previous week's forecast ('v_prior').
1. Use the 'query_logistics_data' tool to write robust SQL queries to aggregate and compare versions by different granularities (e.g., total, by country, by route, by lane_type).
2. Look for large variations between total qty or volume between the versions.
3. Identify any major changes (>10%) by any granularity level.
4. Extract the context: route, country, lane_type.
5. Provide a detailed summary of your findings as actionable insights."""
)

reality_agent = LlmAgent(
    name="RealityChecker",
    model="gemini-2.0-pro-exp",
    tools=[FunctionTool(func=query_logistics_data)],
    output_key="reality_anomalies",
    instruction="""You are a Supply Chain Auditor known as the Reality Checker. Your goal is to compare the recent forecast vs actual transportation data to ensure the forecast is not too far from reality.
1. Use the 'query_logistics_data' tool to join the 'forecasts' (version='v_prior' or 'v_current') and 'actuals' tables on route and date. 
2. Compare the 'qty' and 'actual_qty' at various aggregation levels (e.g., per country, per lane_type, per route).
3. Identify any routes or lane types where the absolute difference exceeds 10%. 
4. Provide a structured report of these historical systemic biases or inaccuracies."""
)

reporter_agent = LlmAgent(
    name="LogisticsReporter",
    model="gemini-2.0-pro-exp",
    instruction="""You are a UI/UX Designer and Logistics Reporter. Your job is to take the variance anomalies and reality gaps found by the other agents and synthesize them into a beautiful, stakeholder-ready HTML dashboard.
1. Produce ONLY valid, complete HTML5 code (using Tailwind CSS via CDN) and no other markdown text.
2. The HTML should feature a professional dashboard design with a dark mode or clear modern aesthetic, clear metric cards for main findings, data tables for specific routes, and actionable recommendations.
3. Combine the 'Variance Anomalies' and 'Reality Anomalies' into distinct sections. Use the variables {variance_anomalies} and {reality_anomalies} if needed."""
)

# ==========================================
# 4. ORCHESTRATE & RUN
# ==========================================
# Step A: Analysts work in parallel
analysis_phase = ParallelAgent(
    name="AnalysisPhase",
    sub_agents=[variance_agent, reality_agent]
)

# Step B: Analysts finish, Reporter takes over
health_check_system = SequentialAgent(
    name="ForecastHealthCheck",
    sub_agents=[analysis_phase, reporter_agent]
)

# Run the system
runner = AgentRunner(agent=health_check_system)
runner.state["db_engine"] = memory_engine # Inject database into shared memory

print("Starting Logistics Health-Check Pipeline...")
report = runner.run("Analyze the newest forecast data and generate the health-check HTML report.")

# Save the final HTML output to a file
with open("logistics_report.html", "w") as f:
    f.write(report.text)

print("Pipeline Complete! Open 'logistics_report.html' in your browser.")