# finding the cost and revenue for each round trip routes and agg the data
import pandas as pd
import numpy as np

OP_COST_LARGE_AIRPORT = 10000
OP_COST_MEDIUM_AIRPORT = 5000
MAX_PASSENGER = 200


def cal_delay_cost(delay: int) -> float:
    """
    calculate the cost of delay
    """
    return 0 if delay <= 15 else (delay - 15) * 75


def finding_cost_revenue(
    routeTrip_flights: pd.DataFrame,
    average_TicketPrice: pd.DataFrame,
    airport_code: pd.DataFrame,
) -> pd.DataFrame:

    ## only look at the round trip flights that associate with the tickets
    routeTrip_flights = routeTrip_flights[
        routeTrip_flights["round_trip_route_IATA"].isin(
            average_TicketPrice["RoundTrip_AIRPORT_IATA_CODE"]
        )
    ]

    routeTrip_flights = routeTrip_flights.merge(
        how="left",
        right=average_TicketPrice,
        left_on="round_trip_route_IATA",
        right_on="RoundTrip_AIRPORT_IATA_CODE",
    )

    routeTrip_flights = routeTrip_flights.assign(
        inbound_average_ticket_price=routeTrip_flights[
            "avg_ticket_price_perPassenger_oneWay"
        ],
        outbound_average_ticket_price=routeTrip_flights[
            "avg_ticket_price_perPassenger_oneWay"
        ],
    )

    ## calculating the cost of each round trip

    # operation cost and overhead cost and delay cost
    routeTrip_flights = routeTrip_flights.assign(
        inbound_operation_cost=routeTrip_flights["inbound_DISTANCE"] * 8,
        outbound_operation_cost=routeTrip_flights["outbound_DISTANCE"] * 8,
        inbound_overhead_cost=routeTrip_flights["inbound_DISTANCE"] * 1.18,
        outbound_overhead_cost=routeTrip_flights["outbound_DISTANCE"] * 1.18,
        inbound_dep_delay_cost=routeTrip_flights["inbound_DEP_DELAY"].apply(
            cal_delay_cost
        ),
        inbound_arr_delay_cost=routeTrip_flights["inbound_ARR_DELAY"].apply(
            cal_delay_cost
        ),
        outbound_dep_delay_cost=routeTrip_flights["outbound_DEP_DELAY"].apply(
            cal_delay_cost
        ),
        outbound_arr_delay_cost=routeTrip_flights["outbound_ARR_DELAY"].apply(
            cal_delay_cost
        ),
    )

    # airport operation cost
    temp = routeTrip_flights.assign(
        temp=routeTrip_flights["round_trip_route_IATA"].apply(lambda x: x[0])
    )
    first_airport_cost = temp.merge(
        airport_code[["AIRPORT_IATA_CODE", "TYPE"]],
        left_on="temp",
        right_on="AIRPORT_IATA_CODE",
        how="left",
    )["TYPE"].apply(
        lambda x: (
            OP_COST_LARGE_AIRPORT if x == "large_airport" else OP_COST_MEDIUM_AIRPORT
        )
    )

    temp = routeTrip_flights.assign(
        temp=routeTrip_flights["round_trip_route_IATA"].apply(lambda x: x[1])
    )
    second_airport_cost = temp.merge(
        airport_code[["AIRPORT_IATA_CODE", "TYPE"]],
        left_on="temp",
        right_on="AIRPORT_IATA_CODE",
        how="left",
    )["TYPE"].apply(
        lambda x: (
            OP_COST_LARGE_AIRPORT if x == "large_airport" else OP_COST_MEDIUM_AIRPORT
        )
    )

    routeTrip_flights = routeTrip_flights.assign(
        airport_operation_cost=first_airport_cost + second_airport_cost
    )

    # total cost
    routeTrip_flights = routeTrip_flights.assign(
        inbound_total_cost=routeTrip_flights[
            [
                "inbound_operation_cost",
                "inbound_overhead_cost",
                "inbound_dep_delay_cost",
                "inbound_arr_delay_cost",
            ]
        ].sum(axis=1),
        outbound_total_cost=routeTrip_flights[
            [
                "outbound_operation_cost",
                "outbound_overhead_cost",
                "outbound_dep_delay_cost",
                "outbound_arr_delay_cost",
            ]
        ].sum(axis=1),
    )
    routeTrip_flights = routeTrip_flights.assign(
        total_cost=routeTrip_flights["inbound_total_cost"]
        + routeTrip_flights["outbound_total_cost"]
        + routeTrip_flights["airport_operation_cost"]
    )

    ## calculating the revenue of each round trip

    # associated passengers
    routeTrip_flights = routeTrip_flights.assign(
        inbound_passengers=routeTrip_flights["inbound_OCCUPANCY_RATE"] * MAX_PASSENGER,
        outbound_passengers=routeTrip_flights["outbound_OCCUPANCY_RATE"]
        * MAX_PASSENGER,
    )

    # tickets revenue
    routeTrip_flights = routeTrip_flights.assign(
        inbound_ticket_revenues=routeTrip_flights["inbound_passengers"]
        * routeTrip_flights["inbound_average_ticket_price"],
        outbound_ticket_revenues=routeTrip_flights["outbound_passengers"]
        * routeTrip_flights["outbound_average_ticket_price"],
    )

    # baggage revenue
    routeTrip_flights = routeTrip_flights.assign(
        inbound_baggage_revenues=routeTrip_flights["inbound_passengers"] * 0.5 * 35,
        outbound_baggage_revenues=routeTrip_flights["outbound_passengers"] * 0.5 * 35,
    )

    # total revenue
    routeTrip_flights = routeTrip_flights.assign(
        inbound_total_revenue=routeTrip_flights["inbound_ticket_revenues"]
        + routeTrip_flights["inbound_baggage_revenues"],
        outbound_total_revenue=routeTrip_flights["outbound_ticket_revenues"]
        + routeTrip_flights["outbound_baggage_revenues"],
    )
    routeTrip_flights = routeTrip_flights.assign(
        total_revenue=routeTrip_flights["inbound_total_revenue"]
        + routeTrip_flights["outbound_total_revenue"]
    )

    ## calculating the profit of each round trip
    routeTrip_flights = routeTrip_flights.assign(
        profit=routeTrip_flights["total_revenue"] - routeTrip_flights["total_cost"]
    )

    return routeTrip_flights
