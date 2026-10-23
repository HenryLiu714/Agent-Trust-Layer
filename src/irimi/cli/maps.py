"""`irimi maps list`: the loaded service maps."""

import argparse
from typing import Any

from irimi import servicemap
from irimi.cli import session
from irimi.report.maps import maps_lines


def register(subparsers: "argparse._SubParsersAction[Any]") -> None:
    maps = subparsers.add_parser("maps", allow_abbrev=False, help="show the loaded service maps")
    maps_sub = maps.add_subparsers(dest="maps_command", metavar="<subcommand>", required=True)
    maps_list = maps_sub.add_parser(
        "list",
        allow_abbrev=False,
        help="print each mapped host with its service, route count and answer target",
    )
    maps_list.set_defaults(func=cmd_maps_list)


def cmd_maps_list(args: argparse.Namespace) -> int:
    index = session.load_maps()
    if index is None:
        return 1
    for line in maps_lines(index, servicemap.override_path()):
        print(line)
    return 0
