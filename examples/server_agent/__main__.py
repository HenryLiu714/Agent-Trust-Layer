"""`python -m examples.server_agent`: the app under its own module name,
`examples.server_agent.app`, so its trigger's entrypoint is importable."""

import sys

from examples.server_agent import app

sys.exit(app.main())
