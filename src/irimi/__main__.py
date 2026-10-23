"""`python -m irimi`: the `irimi` command, for a caller that runs it by its interpreter, as
`sdk.replay()` does. Only when run: importing it runs nothing."""

import sys

from irimi.cli import main

if __name__ == "__main__":
    sys.exit(main())
