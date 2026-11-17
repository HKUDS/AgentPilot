from .sandbox import BaseSandbox, SandboxManager
from .policies import SecurityPolicyManager, SecurityPolicy

# Try to import E2BSandbox (optional dependency)
try:
    from .e2b_sandbox import E2BSandbox
    E2B_AVAILABLE = True
except ImportError:
    E2BSandbox = None
    E2B_AVAILABLE = False

# Try to import BoxdSandbox (optional dependency)
try:
    from .boxd_sandbox import BoxdSandbox, BOXD_AVAILABLE
except ImportError:
    BoxdSandbox = None
    BOXD_AVAILABLE = False

__all__ = [
    "BaseSandbox",
    "SandboxManager",
    "SecurityPolicyManager",
    "SecurityPolicy"
]

if E2B_AVAILABLE:
    __all__.append("E2BSandbox")

if BOXD_AVAILABLE:
    __all__.append("BoxdSandbox")