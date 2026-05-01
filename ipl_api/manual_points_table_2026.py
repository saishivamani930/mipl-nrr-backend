from __future__ import annotations

from typing import Dict, TypedDict

from ipl_api.nrr_math import overs_to_balls


class ManualAggregate(TypedDict):
    runs_for: int
    balls_for: int
    runs_against: int
    balls_against: int


def add_overs(*overs_list: str) -> str:
    """
    Add cricket overs correctly.

    Example:
        add_overs("156.1", "18.5") -> "175.0"
    """
    total_balls = 0

    for overs in overs_list:
        total_balls += overs_to_balls(overs)

    return f"{total_balls // 6}.{total_balls % 6}"


def agg(
    *,
    runs_for: int,
    overs_for: str,
    runs_against: int,
    overs_against: str,
) -> ManualAggregate:
    return {
        "runs_for": runs_for,
        "balls_for": overs_to_balls(overs_for),
        "runs_against": runs_against,
        "balls_against": overs_to_balls(overs_against),
    }


MANUAL_AGGREGATES_2026: Dict[str, ManualAggregate] = {
    "PBKS": agg(
        runs_for=1537,
        overs_for=("132.0"),
        runs_against=1477,
        overs_against=("139.2"),
    ),

    "RCB": agg(
        runs_for=1656,
        overs_for=("156.1"),
        runs_against=1595,
        overs_against=("173.4"),
    ),

    "SRH": agg(
        runs_for=1932,
        overs_for=("177.1"),
        runs_against=1756,
        overs_against=("174.2"),
    ),

    "RR": agg(
        runs_for=1619,
        overs_for=("160.3"),
        runs_against=1602,
        overs_against=("169.1"),
    ),

    "GT": agg(
        runs_for=1547,
        overs_for=("170.5"),
        runs_against=1646,
        overs_against=("178.0"),
    ),

    "CSK": agg(
        runs_for=1496,
        overs_for=("160.0"),
        runs_against=1397,
        overs_against=("147.3"),
    ),

    "DC": agg(
        runs_for=1420,
        overs_for=("155.1"),
        runs_against=1484,
        overs_against=("145.2"),
    ),

    "KKR": agg(
        runs_for=1218,
        overs_for=("139.4"),
        runs_against=1315,
        overs_against=("138.5"),
    ),

    "MI": agg(
        runs_for=1472,
        overs_for=("150.1"),
        runs_against=1528,
        overs_against=("144.2"),
    ),

    "LSG": agg(
        runs_for=1267,
        overs_for=("159.5"),
        runs_against=1364,
        overs_against=("151.0"),
    ),
}