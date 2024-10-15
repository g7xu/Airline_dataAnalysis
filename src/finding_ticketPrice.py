# finding the average ticket price of a round trip
import pandas as pd


def finding_avg_roundtripTickets(tickets: pd.DataFrame) -> pd.DataFrame:
    """
    finding the average ticket price of the
    """
    tickets = tickets.assign(
        sorted_route=tickets.apply(
            lambda x: tuple(
                sorted([x["ORIGIN_AIRPORT_IATA_CODE"], x["DEST_AIRPORT_IATA_CODE"]])
            ),
            axis=1,
        )
    )
    tickets = tickets.drop(
        ["ORIGIN_AIRPORT_IATA_CODE", "DEST_AIRPORT_IATA_CODE"], axis=1
    )
    # avg_price = (
    #     tickets.groupby(["sorted_route", "OP_CARRIER"])[["ONE_PASSENGERS_FARE"]]
    #     .mean()
    #     .reset_index()
    #     .rename(
    #         columns={
    #             "sorted_route": "round_trip_route_IATA",
    #             "ONE_PASSENGERS_FARE": "average_ticket_price_op",
    #         }
    #     )
    # )

    # avg_price = avg_price.assign(
    #     average_ticket_price_routes=avg_price.groupby("round_trip_route_IATA")[
    #         "average_ticket_price_op"
    #     ].transform("mean")
    # )

    # return avg_price

    avg_price = (
        tickets.groupby("sorted_route")[["ONE_PASSENGERS_FARE"]]
        .mean()
        .reset_index()
        .rename(
            columns={
                "sorted_route": "round_trip_route_IATA",
                "ONE_PASSENGERS_FARE": "average_ticket_price_routes",
            }
        )
    )

    avg_price = avg_price.assign(
        average_ticket_price_routes=avg_price["average_ticket_price_routes"] / 2
    )
    return avg_price


def main():
    pass


if __name__ == "__main__":
    main()
