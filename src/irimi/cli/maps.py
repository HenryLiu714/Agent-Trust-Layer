"""`irimi maps list`: the loaded service maps."""

import argparse
from typing import Any

from irimi import servicemaps
from irimi.cli import compose
from irimi.cli.options import add_map_options, epilog, help_formatted
from irimi.core import exitcodes
from irimi.report.maps import maps_lines


def register(subparsers: "argparse._SubParsersAction[Any]") -> None:
    maps = subparsers.add_parser(
        "maps",
        **help_formatted(
            help="show the service maps irimi loads",
            description="Show the service maps irimi loads. With no subcommand, list them.",
            epilog=epilog(["irimi maps"]),
        ),
    )
    maps_sub = maps.add_subparsers(dest="maps_command", metavar="<subcommand>", required=True)
    maps_list = maps_sub.add_parser(
        "list",
        **help_formatted(
            help="list each mapped host with its service, route count and target",
            description="List each mapped host with its service, route count and target. The "
            "map options preview\nwhat a --target would change.",
            epilog=epilog(
                ["irimi maps list", "irimi maps list --target api.stripe.com=http://127.0.0.1:3000"]
            ),
        ),
    )
    add_map_options(maps_list)
    maps_list.set_defaults(func=cmd_maps_list)


def cmd_maps_list(args: argparse.Namespace) -> int:
    index = compose.load_maps(frozenset(args.allow_remote_target), args.target, args.target_reads)
    if index is None:
        return exitcodes.ERROR
    for line in maps_lines(index, servicemaps.override_path()):
        print(line)
    return exitcodes.OK
