# producing round trip tickets and flights that associate with the large and medium airports in the US.


import pandas as pd
import numpy as np
import re


def airport_size_associate(info_df: pd.DataFrame, airport_df: pd.DataFrame):
    """
    associate airport kinds with the airport codes in info_df
    ---
    info_df: pandas DataFrame contain information of the airport
    airport_df: pandas DataFrame contain information regarding airports's kind
    """
    flights_airports = (
        info_df.merge(
            airport_df[["TYPE", "IATA_CODE"]],
            how="inner",
            left_on="ORIGIN_AIRPORT_IATA",
            right_on="IATA_CODE",
        )
        .rename(columns={"TYPE": "ORIGIN_AIRPORT_TYPE"})
        .drop(columns="IATA_CODE")
        .merge(
            airport_df[["TYPE", "IATA_CODE"]],
            how="inner",
            left_on="DEST_AIRPORT_IATA",
            right_on="IATA_CODE",
        )
        .rename(columns={"TYPE": "DESTINATION_AIRPORT_TYPE"})
        .drop(columns="IATA_CODE")
    )

    return flights_airports


def round_trip_US_flights(flights_df: pd.DataFrame, airports_df: pd.DataFrame):
    """
    Producing round trip flights that associate with the large and medium airports in the US.
    ---
    flights_df: pandas DataFrame contain information of the flights
    airports_df: pandas DataFrame contain information regarding airports's kind
    """
    # query flights that trip between large and medium airports
    flights_airport = airport_size_associate(flights_df, airports_df)

    flights_LMairport = flights_airport[
        flights_airport["ORIGIN_AIRPORT_TYPE"]
        != flights_airport["DESTINATION_AIRPORT_TYPE"]
    ]

    # self merge the dataframe to get round trip flights
    round_trips = flights_LMairport.merge(
        flights_LMairport,
        left_on=[
            "OP_CARRIER_IATA_CODE",
            "TAIL_NUM",
            "OP_CARRIER_FL_NUM",
            "DEST_AIRPORT_IATA",
        ],
        right_on=[
            "OP_CARRIER_IATA_CODE",
            "TAIL_NUM",
            "OP_CARRIER_FL_NUM",
            "ORIGIN_AIRPORT_IATA",
        ],
    )

    # query valid round trip flights
    round_trips = round_trips[
        (
            abs(round_trips["FL_DATE_x"] - round_trips["FL_DATE_y"])
            < pd.Timedelta("1 days")
        )
        & (round_trips["ORIGIN_AIRPORT_IATA_x"] == round_trips["DEST_AIRPORT_IATA_y"])
    ]
    # reducing duplicates and creating columns on the round_trip_routes
    round_trips = round_trips.assign(
        sorted_route=round_trips.apply(
            lambda x: tuple(
                sorted([x["ORIGIN_AIRPORT_IATA_x"], x["DEST_AIRPORT_IATA_x"]])
            ),
            axis=1,
        )
    )
    return round_trips.drop_duplicates(
        subset=[
            "FL_DATE_x",
            "OP_CARRIER_IATA_CODE",
            "TAIL_NUM",
            "OP_CARRIER_FL_NUM",
            "sorted_route",
        ]
    )


def round_trip_US_tickets(tickets_df: pd.DataFrame, airports_df: pd.DataFrame):
    """
    Processing tickets associated with the large and medium airports in the US.
    ---
    tickets_df: pandas DataFrame contain information of the tickets
    airports_df: pandas DataFrame contain information regarding airports's kind
    """
    tickets_airport = airport_size_associate(tickets_df, airports_df)

    # only speculate tickets between large and medium airports
    tickets_LMairport = tickets_airport[
        tickets_airport["ORIGIN_AIRPORT_TYPE"]
        != tickets_airport["DESTINATION_AIRPORT_TYPE"]
    ]

    return tickets_LMairport.assign(
        sorted_route=tickets_LMairport.apply(
            lambda x: tuple(sorted([x["ORIGIN_AIRPORT_IATA"], x["DEST_AIRPORT_IATA"]])),
            axis=1,
        )
    )


def main():
    flights = pd.read_csv("data/cleaned_data/flights.csv")
    flights["FL_DATE"] = pd.to_datetime(flights["FL_DATE"])
    tickets = pd.read_csv("data/cleaned_data/Tickets.csv")
    airportsInfo = pd.read_csv("data/cleaned_data/Airport_Codes.csv")

    round_trip_US_flights(flights, airportsInfo).to_csv(
        "data/muggling_data/round_trip_flights.csv", index=False
    )
    round_trip_US_tickets(tickets, airportsInfo).to_csv(
        "data/muggling_data/round_trip_tickets.csv", index=False
    )


if __name__ == "__main__":
    main()
