"""`python -m irimi`: the `irimi` command, for a caller that runs it by its interpreter, as
`sdk.replay()` does (#84)."""

import sys

from irimi.cli import main

sys.exit(main())
