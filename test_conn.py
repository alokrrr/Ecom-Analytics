
import sys
sys.path.append(".")
try:
    import backend.api.anomalies as mod
    print("Module imported:", mod)
    print("Has attribute 'router':", hasattr(mod, "router"))
    router = getattr(mod, "router", None)
    print("Router repr:", router)
except Exception as e:
    import traceback
    traceback.print_exc()
