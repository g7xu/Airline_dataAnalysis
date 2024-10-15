from src.finding_roundtripFlights import *
import pandas as pd
import numpy as np

syn_fl = pd.read_csv("data/test_data/Synthetic_Flights.csv")
syn_airport = pd.read_csv("data/test_data/Synthetic_Airport_Codes.csv")


def test_updating_rt_candidate_addingRoutes() -> None:
    rt_candidate = {("A", "B"): [pd.Series([1, 2, 3])]}

    updating_rt_candidate(("A", "B"), rt_candidate, pd.Series([4, 5, 6]))

    assert rt_candidate.keys() == {
        ("A", "B")
    }, "The key should not be changed, when adding routes to pre-exist key"
    assert rt_candidate[("A", "B")][0].equals(
        pd.Series([1, 2, 3])
    ), "The previous route should not be changed"
    assert rt_candidate[("A", "B")][1].equals(
        pd.Series([4, 5, 6])
    ), "The new route should be added to the list"

    updating_rt_candidate(("A", "B"), rt_candidate, pd.Series([1]))
    assert rt_candidate[("A", "B")][0].equals(
        pd.Series([1, 2, 3])
    ), "The previous route should not be changed"
    assert rt_candidate[("A", "B")][1].equals(
        pd.Series([4, 5, 6])
    ), "The new route should be added to the list"
    assert rt_candidate[("A", "B")][2].equals(
        pd.Series([1])
    ), "The new route should be added to the list"

    updating_rt_candidate(("D", "C"), rt_candidate, pd.Series([1, 2, 3]))
    assert rt_candidate.keys() == {
        ("A", "B"),
        ("D", "C"),
    }, "The new key should be added"
    assert rt_candidate[("D", "C")][0].equals(
        pd.Series([1, 2, 3])
    ), "The new route should be added to the list"


def test_adding_rt_routes() -> None:
    # rt_candidate = {("A", "B"): [pd.Series({"row1": 1, "row2": 2, "row3": 3})]}
    rt_candidate = ("A", "B")
    rt_routes = []
    route1 = pd.Series({"row1": 1, "row2": 2, "row3": 3})
    route2 = pd.Series({"row1": 4, "row2": 5, "row3": 6})

    adding_rt_routes(rt_candidate, rt_routes, route1, route2)

    assert (
        isinstance(rt_routes, list) and len(rt_routes) == 1
    ), "The rt_routes should be a list with one element"
    assert isinstance(
        rt_routes[0], pd.DataFrame
    ), "The element in the list should be a DataFrame"
    assert rt_routes[0].shape[0] == 1, "The DataFrame should have one row"
    assert rt_routes[0].shape[1] == 7, "The DataFrame should have 6 columns"
    assert pd.DataFrame(
        data=[[1, 2, 3, 4, 5, 6, ("A", "B")]],
        columns=[
            "inbound_row1",
            "inbound_row2",
            "inbound_row3",
            "outbound_row1",
            "outbound_row2",
            "outbound_row3",
            "round_trip_route_IATA",
        ],
    ).equals(rt_routes[0]), "The DataFrame should have the correct values"

    adding_rt_routes(("B", "A"), rt_routes, route2, route1)

    assert pd.DataFrame(
        data=[[1, 2, 3, 4, 5, 6, ("A", "B")]],
        columns=[
            "inbound_row1",
            "inbound_row2",
            "inbound_row3",
            "outbound_row1",
            "outbound_row2",
            "outbound_row3",
            "round_trip_route_IATA",
        ],
    ).equals(rt_routes[1]), "The DataFrame should have the correct values"

    assert pd.DataFrame(
        data=[[1, 2, 3, 4, 5, 6, ("A", "B")]],
        columns=[
            "inbound_row1",
            "inbound_row2",
            "inbound_row3",
            "outbound_row1",
            "outbound_row2",
            "outbound_row3",
            "round_trip_route_IATA",
        ],
    ).equals(rt_routes[0]), "The DataFrame should have the correct values"


def test_interaction_adding_round_trip_route_from_dict() -> None:
    rt_candidate_dict = dict()
    rt_routes = []

    for index, row in syn_fl[syn_fl["TAIL_NUM"] == "B700DB"].iterrows():
        updating_rt_candidate(("WDU", "NZR"), rt_candidate_dict, row)

    assert len(rt_candidate_dict) == 1, "There should be one key in the dictionary"
    assert rt_candidate_dict.keys() == {
        ("WDU", "NZR")
    }, "The key should be ('WDU', 'NZR')"

    assert isinstance(rt_candidate_dict[("WDU", "NZR")], list)
    assert isinstance(rt_candidate_dict[("WDU", "NZR")][0], pd.Series)
    assert rt_candidate_dict[("WDU", "NZR")][0].equals(
        syn_fl[syn_fl["TAIL_NUM"] == "B700DB"].iloc[0]
    ), "problem in creating value in round-trip route dict"

    for index, row in syn_fl[syn_fl["TAIL_NUM"] == "B008UD"].iterrows():
        updating_rt_candidate(("specialA", "specialB"), rt_candidate_dict, row)

    for i in range(0, syn_fl[syn_fl["TAIL_NUM"] == "B008UD"].shape[0]):
        assert rt_candidate_dict[("specialA", "specialB")][i].equals(
            syn_fl[syn_fl["TAIL_NUM"] == "B008UD"].iloc[i]
        ), "problem in updating value in the round-trip route dict"

    bud_count = 5
    for i in range(0, syn_fl[syn_fl["TAIL_NUM"] == "B008UD"].shape[0]):
        adding_rt_routes(
            ("specialA", "specialB"),
            rt_routes,
            syn_fl[syn_fl["TAIL_NUM"] == "B008UD"].iloc[i],
            syn_fl[syn_fl["TAIL_NUM"] == "B008UD"].iloc[i],
        )

        assert (
            rt_routes[i]
            .reset_index(drop=True)
            .equals(
                pd.concat(
                    [
                        pd.DataFrame(syn_fl[syn_fl["TAIL_NUM"] == "B008UD"].iloc[i])
                        .T.add_prefix("inbound_")
                        .reset_index(drop=True),
                        pd.DataFrame(syn_fl[syn_fl["TAIL_NUM"] == "B008UD"].iloc[i])
                        .T.add_prefix("outbound_")
                        .reset_index(drop=True),
                        pd.DataFrame(
                            {"round_trip_route_IATA": [("specialA", "specialB")]}
                        ),
                    ],
                    axis=1,
                ).reset_index(drop=True)
            )
        )


def test_finding_roundtripFlights_oneTAIL_NUM() -> None:
    round_trip_result_B43543UD, sole_trip_result_B43543UD = finding_roundtripFlights(
        syn_fl[syn_fl["TAIL_NUM"] == "B43543UD"]
    )

    assert isinstance(
        round_trip_result_B43543UD, pd.DataFrame
    ), "The result should be a DataFrame"
    assert isinstance(
        sole_trip_result_B43543UD, pd.DataFrame
    ), "The result should be a DataFrame"

    assert (
        round_trip_result_B43543UD.shape[0] == 4
        and sole_trip_result_B43543UD.shape[0] == 2
    ), "Problem in allocating routes"

    assert round_trip_result_B43543UD.reset_index(drop=True)[
        [
            "inbound_FL_DATE",
            "inbound_ORIGIN_AIRPORT_IATA_CODE",
            "inbound_DEST_AIRPORT_IATA_CODE",
            "outbound_FL_DATE",
            "outbound_ORIGIN_AIRPORT_IATA_CODE",
            "outbound_DEST_AIRPORT_IATA_CODE",
            "round_trip_route_IATA",
        ]
    ].equals(
        pd.DataFrame(
            {
                "inbound_FL_DATE": [
                    "2020-02-12",
                    "2020-02-29",
                    "2020-03-04",
                    "2020-03-07",
                ],
                "inbound_ORIGIN_AIRPORT_IATA_CODE": ["RSZ", "RSZ", "RSZ", "LWI"],
                "inbound_DEST_AIRPORT_IATA_CODE": ["VID", "VID", "VID", "RSZ"],
                "outbound_FL_DATE": [
                    "2020-02-18",
                    "2020-03-02",
                    "2020-03-04",
                    "2020-02-24",
                ],
                "outbound_ORIGIN_AIRPORT_IATA_CODE": ["VID", "VID", "VID", "RSZ"],
                "outbound_DEST_AIRPORT_IATA_CODE": ["RSZ", "RSZ", "RSZ", "LWI"],
                "round_trip_route_IATA": [
                    ("RSZ", "VID"),
                    ("RSZ", "VID"),
                    ("RSZ", "VID"),
                    ("LWI", "RSZ"),
                ],
            }
        )
    ), "incorrect data in route_trip output"

    assert sole_trip_result_B43543UD.reset_index(drop=True)[
        ["FL_DATE", "FL_NUM", "ORIGIN_AIRPORT_IATA_CODE", "DEST_AIRPORT_IATA_CODE"]
    ].equals(
        pd.DataFrame(
            {
                "FL_DATE": ["2020-02-29", "2020-03-06"],
                "FL_NUM": ["XI762", "XI6138"],
                "ORIGIN_AIRPORT_IATA": ["RWZ", "RSZ"],
                "DEST_AIRPORT_IATA": ["RSZ", "LWI"],
            }
        )
    )


def test_finding_roundtripFlights_MULT_TAIL_NUM() -> None:
    two_tails = syn_fl[
        (syn_fl["TAIL_NUM"] == "B43543UD") | (syn_fl["TAIL_NUM"] == "B008UD")
    ]

    round_trip_result, sole_trips = finding_roundtripFlights(two_tails)

    assert isinstance(
        round_trip_result, pd.DataFrame
    ), "The result should be a DataFrame"
    assert isinstance(sole_trips, pd.DataFrame), "The result should be a DataFrame"

    assert (
        round_trip_result.shape[0] == 6 and sole_trips.shape[0] == 4
    ), "Problem in allocating routes"

    assert (
        round_trip_result[
            [
                "inbound_FL_DATE",
                "inbound_ORIGIN_AIRPORT_IATA",
                "inbound_DEST_AIRPORT_IATA",
                "outbound_FL_DATE",
                "outbound_ORIGIN_AIRPORT_IATA",
                "outbound_DEST_AIRPORT_IATA",
                "round_trip_route_IATA",
            ]
        ]
        .reset_index(drop=True)
        .equals(
            pd.DataFrame(
                {
                    "inbound_FL_DATE": [
                        "2020-02-17",
                        "2020-03-05",
                        "2020-02-12",
                        "2020-02-29",
                        "2020-03-04",
                        "2020-03-07",
                    ],
                    "inbound_ORIGIN_AIRPORT_IATA": [
                        "LWI",
                        "HLU",
                        "RSZ",
                        "RSZ",
                        "RSZ",
                        "LWI",
                    ],
                    "inbound_DEST_AIRPORT_IATA": [
                        "VID",
                        "VID",
                        "VID",
                        "VID",
                        "VID",
                        "RSZ",
                    ],
                    "outbound_FL_DATE": [
                        "2020-02-17",
                        "2020-03-05",
                        "2020-02-18",
                        "2020-03-02",
                        "2020-03-04",
                        "2020-02-24",
                    ],
                    "outbound_ORIGIN_AIRPORT_IATA": [
                        "VID",
                        "VID",
                        "VID",
                        "VID",
                        "VID",
                        "RSZ",
                    ],
                    "outbound_DEST_AIRPORT_IATA": [
                        "LWI",
                        "HLU",
                        "RSZ",
                        "RSZ",
                        "RSZ",
                        "LWI",
                    ],
                    "round_trip_route_IATA": [
                        ("LWI", "VID"),
                        ("HLU", "VID"),
                        ("RSZ", "VID"),
                        ("RSZ", "VID"),
                        ("RSZ", "VID"),
                        ("LWI", "RSZ"),
                    ],
                }
            )
        )
    ), "incorrect data in route_trip output"

    assert (
        sole_trips[["FL_DATE", "FL_NUM", "ORIGIN_AIRPORT_IATA", "DEST_AIRPORT_IATA"]]
        .reset_index(drop=True)
        .equals(
            pd.DataFrame(
                {
                    "FL_DATE": ["2020-02-28", "2020-02-29", "2020-03-04", "2020-03-06"],
                    "FL_NUM": ["XI2133", "XI762", "XI1343", "XI6138"],
                    "ORIGIN_AIRPORT_IATA": ["LWI", "RWZ", "RWZ", "RSZ"],
                    "DEST_AIRPORT_IATA": ["VID", "RSZ", "HLU", "LWI"],
                }
            )
        )
    ), "incorrect data in sole role output"


def test_finding_round_trip_of_different_tail_num() -> None:
    round_trip, sole_trip = finding_roundtripFlights(syn_fl.iloc[[1, 15], :])

    assert isinstance(round_trip, pd.DataFrame), "The result should be a DataFrame"

    assert round_trip.shape[0] == 1, "Problem in allocating routes"

    assert (
        round_trip[
            [
                "inbound_FL_DATE",
                "inbound_ORIGIN_AIRPORT_IATA",
                "inbound_DEST_AIRPORT_IATA",
                "outbound_FL_DATE",
                "outbound_ORIGIN_AIRPORT_IATA",
                "outbound_DEST_AIRPORT_IATA",
                "round_trip_route_IATA",
            ]
        ]
        .reset_index(drop=True)
        .equals(
            pd.DataFrame(
                {
                    "inbound_FL_DATE": ["2020-02-29"],
                    "inbound_ORIGIN_AIRPORT_IATA": ["LWI"],
                    "inbound_DEST_AIRPORT_IATA": ["VID"],
                    "outbound_FL_DATE": ["2020-02-17"],
                    "outbound_ORIGIN_AIRPORT_IATA": ["VID"],
                    "outbound_DEST_AIRPORT_IATA": ["LWI"],
                    "round_trip_route_IATA": [("LWI", "VID")],
                }
            )
        )
    ), "problem with sole trips"
