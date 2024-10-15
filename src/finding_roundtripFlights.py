import pandas as pd
import numpy as np


def updating_rt_candidate(
    round_trip_IATA: tuple, rt_candidate_dict: dict, route: pd.Series
) -> None:
    """
    updating the roundtrip candidate dictionary by adding or removing the routes from the df of the corresponding roundtrip key

    Parameters:
    rt_candidate_dict: dict
        the dictionary of roundtrip candidates
    route: pd.Series
        the route to be added or removed
    removing: bool
        whether to remove the route from the roundtrip candidate dictionary

    Returns:
    None: the operation will be done in place
    """
    # adding route Series into the dictionary
    if round_trip_IATA not in rt_candidate_dict:
        rt_candidate_dict[round_trip_IATA] = [route]
    else:
        rt_candidate_dict[round_trip_IATA].append(route)


def adding_rt_routes(
    round_trip_IATA: tuple,
    rt_routes: list,
    inbound_route: pd.Series,
    outbound_route: pd.Series,
) -> None:
    """
    updating the roundtrip routes dataframe by adding or removing the routes from the df of the corresponding roundtrip key

    Parameters:
    round_trip_IATA: tuple
        the tuple of the roundtrip IATA codes
    rt_routes: pd.DataFrame
        the dataframe of roundtrip routes
    inbound_route: pd.Series
        the inbound route to be added or removed
    outbound_route: pd.Series
        the outbound route to be added or removed
    """
    if round_trip_IATA[0] > round_trip_IATA[1]:
        inbound_route, outbound_route = outbound_route, inbound_route
        round_trip_IATA = (round_trip_IATA[1], round_trip_IATA[0])

    rt_routes_row = pd.concat(
        [
            pd.DataFrame(inbound_route).T.add_prefix("inbound_").reset_index(drop=True),
            pd.DataFrame(outbound_route)
            .T.add_prefix("outbound_")
            .reset_index(drop=True),
        ],
        axis=1,
    )

    rt_routes.append(rt_routes_row.assign(round_trip_route_IATA=[round_trip_IATA]))


def finding_roundtripFlights(flights: pd.DataFrame) -> pd.DataFrame:
    """
    finding the roundtrip flights in the flights data
    rt stands for roundtrip

    Parameters:
    flights: pd.DataFrame
        the flights data

    Returns:
    pd.DataFrame
        the roundtrip flights
    """
    rt_routes = []
    sole_routes = []

    def updating_roundtrips(tail_num_df: pd.DataFrame) -> None:
        rt_candidate_dict = dict()
        # sorting the flights by date
        sorted_tail_num_df = tail_num_df.sort_values("FL_DATE", ascending=True)

        # loop through each flights
        for index, route in sorted_tail_num_df.iterrows():
            round_trip_candidate = (
                route["ORIGIN_AIRPORT_IATA_CODE"],
                route["DEST_AIRPORT_IATA_CODE"],
            )
            corr_round_trip_candidate = (
                route["DEST_AIRPORT_IATA_CODE"],
                route["ORIGIN_AIRPORT_IATA_CODE"],
            )
            if (
                corr_round_trip_candidate not in rt_candidate_dict
                or len(rt_candidate_dict[corr_round_trip_candidate]) == 0
            ):
                updating_rt_candidate(round_trip_candidate, rt_candidate_dict, route)
            else:
                adding_rt_routes(
                    round_trip_candidate,
                    rt_routes,
                    route,
                    rt_candidate_dict[corr_round_trip_candidate].pop(0),
                )

        for key, value in rt_candidate_dict.items():
            if isinstance(value, list) and len(value) > 0:
                sole_routes.extend(value)

    for index, tail_num_df in flights.groupby("TAIL_NUM"):
        updating_roundtrips(tail_num_df)

    p_sole_routes = pd.concat(sole_routes, axis=1).T if len(sole_routes) > 0 else None
    sole_routes = []
    updating_roundtrips(p_sole_routes)

    return (
        pd.concat(rt_routes, axis=0) if len(rt_routes) > 0 else None,
        pd.concat(sole_routes, axis=1).T if len(sole_routes) > 0 else None,
    )
