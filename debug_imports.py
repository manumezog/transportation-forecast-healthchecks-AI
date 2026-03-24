import google.adk
try:
    import google.adk.agents as agents
    print("Agents:", dir(agents))
except Exception as e:
    print("Agents error:", e)

try:
    import google.adk.runtime as runtime
    print("Runtime:", dir(runtime))
except Exception as e:
    print("Runtime error:", e)

try:
    import google.adk.runners as runners
    print("Runners:", dir(runners))
except Exception as e:
    print("Runners error:", e)
