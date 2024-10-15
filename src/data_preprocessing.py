# the preprocessing script won't remove missing value
# it will
# 1. remove all the cancelled flights
# 2. only look at the round trip tickets
# 3. constrain the range of certain column (ex. distance can not be negative)
# 4. convert the data type of certain column (ex. convert string to float)
# 5. adjust the column name to be more appropriate
# TODO: needs to handle missing value
import pandas as pd
import numpy as np
import re


def flight_float_conversion(text):
    try:
        return abs(float(text))
    except:
        return np.nan


def find_number(text):
    """
    Find the first number in a string
    """
    if type(text) != str:
        return np.nan
    re_result = re.search(r"[\d\.]+", text)
    if re_result is not None:
        return float(re_result.group(0))
    return np.nan


def preprocess_flights(flights: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocessing the flights data that
    1. remove all the cancelled flights
    2. constrain the range of certain column (ex. distance can not be negative)
    3. convert the data type of certain column (ex. convert string to float)
    4. adjust the column name to be more appropriate (joint carrier and flight numerber)
    5. ensure atomic data set
    ---
    flights: pandas DataFrame contain information of the flights
    """
    # only look at flights that is not cancelled
    flights = flights[flights["CANCELLED"] == 0]

    ## adjusting data type of certain columns
    flights = flights.assign(FL_DATE=pd.to_datetime(flights["FL_DATE"]))
    flights = flights.assign(OP_CARRIER=flights["OP_CARRIER"].astype(str))
    flights = flights.assign(OP_CARRIER_FL_NUM=flights["OP_CARRIER_FL_NUM"].astype(str))
    flights = flights.assign(
        AIR_TIME=flights["AIR_TIME"].apply(flight_float_conversion)
    )
    flights = flights.assign(
        DISTANCE=flights["DISTANCE"].apply(flight_float_conversion)
    )

    ## split certain column into two
    split_ORIGIN_CITY_STATE = flights["ORIGIN_CITY_NAME"].str.split(", ")
    split_DEST_CITY_STATE = flights["DEST_CITY_NAME"].str.split(", ")

    flights = flights.assign(ORIGIN_CITY_NAME=split_ORIGIN_CITY_STATE.str[0])
    flights = flights.assign(ORIGIN_STATE_NAME=split_ORIGIN_CITY_STATE.str[1])

    flights = flights.assign(DEST_STATE_NAME=split_DEST_CITY_STATE.str[1])
    flights = flights.assign(DEST_CITY_NAME=split_DEST_CITY_STATE.str[0])

    ## combine certain column into one
    flights = flights.assign(
        FL_NUM=flights["OP_CARRIER"] + flights["OP_CARRIER_FL_NUM"]
    )

    ## adjusting column name
    flights.rename(columns={"ORIGIN": "ORIGIN_AIRPORT_IATA"}, inplace=True)
    flights.rename(columns={"DESTINATION": "DEST_AIRPORT_IATA"}, inplace=True)

    ## handle outliers by replacing the outlier with NaN
    # handle the outliers of delay
    flights = flights[
        (flights["DEP_DELAY"] <= 1440)
        & (flights["DEP_DELAY"] >= -1440)
        & (flights["ARR_DELAY"] <= 1440)
        & (flights["ARR_DELAY"] >= -1440)
    ]
    # handle the outliers of distance
    flights = flights[flights["DISTANCE"] <= 5100]

    # handle the outliers of air time
    flights = flights[flights["AIR_TIME"] <= 660]

    return flights.drop_duplicates()


def preprocess_tickets(tickets: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocessing the tickets data that
    1. only look at tickets that is round trip
    2. adjusting data type of certain columns
    3. adjusting column name
    4. dropping unnesserary columns
    ---
    tickets: pandas DataFrame contain information of the tickets
    """
    ## only look at tickets that is round trip
    tickets = tickets[tickets["ROUNDTRIP"] == 1]

    ## adjusting data type of certain columns
    tickets = tickets.assign(YEAR=tickets["YEAR"].astype(int))
    tickets = tickets.assign(ITIN_FARE=tickets["ITIN_FARE"].apply(find_number))

    ## only look at the tickets that has fare
    tickets = tickets[tickets["ITIN_FARE"] != 0]

    ## adjusting column name
    tickets.rename(columns={"ORIGIN": "ORIGIN_AIRPORT_IATA"}, inplace=True)
    tickets.rename(columns={"DESTINATION": "DEST_AIRPORT_IATA"}, inplace=True)

    tickets = tickets.assign(
        ONE_PASSENGERS_FARE=tickets["ITIN_FARE"] / tickets["PASSENGERS"]
    )

    ## handle outliers by replacing the outlier with NaN

    # handle passenger fare with less than 5000 dollars
    tickets = tickets[tickets["ONE_PASSENGERS_FARE"] <= 5000]

    return tickets.drop_duplicates()


def preprocess_airport_codes(airport_codes: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocessing the airport codes data that
    1. ignore the airport that has no IATA code and not in US and only look at medium and large airport
    2. split oordinates into two column
    3. dropping unnesserary columns
    ---
    airport_codes: pandas DataFrame contain information of the airport codes
    """
    ## ignore the airport that has no IATA code and not in US and only look at medium and large airport
    airport_codes = airport_codes[
        (
            (airport_codes["TYPE"] == "medium_airport")
            | (airport_codes["TYPE"] == "large_airport")
        )
        & (airport_codes["ISO_COUNTRY"] == "US")
        & (airport_codes["IATA_CODE"].notnull())
    ]

    ## split oordinates into two column
    airport_codes = airport_codes.assign(
        COORDINATES_LONGITUDE=airport_codes["COORDINATES"]
        .apply(lambda x: x.split(", ")[0])
        .astype(float)
    )
    airport_codes = airport_codes.assign(
        COORDINATES_LATITUDE=airport_codes["COORDINATES"]
        .apply(lambda x: x.split(", ")[1])
        .astype(float)
    )

    airport_codes = airport_codes.drop_duplicates()

    # handle special case
    # hard code to fix the wrong coordinates
    tofix_index = airport_codes[airport_codes["IATA_CODE"] == "SYA"].index
    airport_codes.loc[tofix_index, "COORDINATES_LONGITUDE"] = -174.113998

    return airport_codes


def impuatate_missing_value_flights(flights: pd.DataFrame) -> pd.DataFrame:

    return flights


def standardlize_datasets(
    flights: pd.DataFrame, tickets: pd.DataFrame, airport_codes: pd.DataFrame
) -> tuple:
    """
    Standardlize the datasets by
    1. ensure all the foregin attributes have the same column name
    2. Flights and ticket dataframe has only the rows related to the airport codes in airport data set
    ---
    flights: pandas DataFrame contain information of the flights
    tickets: pandas DataFrame contain information of the tickets
    airport_codes: pandas DataFrame contain information of the airport codes
    """

    ## standarlize the datasests' column name
    airport_codes = airport_codes.rename(columns={"IATA_CODE": "AIRPORT_IATA_CODE"})

    airport_codes = airport_codes[
        [
            "AIRPORT_IATA_CODE",
            "NAME",
            "TYPE",
            "MUNICIPALITY",
            "ELEVATION_FT",
            "COORDINATES_LONGITUDE",
            "COORDINATES_LATITUDE",
        ]
    ]

    flights = flights.rename(
        columns={
            "ORIGIN_AIRPORT_IATA": "ORIGIN_AIRPORT_IATA_CODE",
            "DEST_AIRPORT_IATA": "DEST_AIRPORT_IATA_CODE",
        }
    )

    flights = flights[
        [
            "FL_DATE",
            "FL_NUM",
            "OP_CARRIER",
            "TAIL_NUM",
            "ORIGIN_AIRPORT_IATA_CODE",
            "ORIGIN_CITY_NAME",
            "ORIGIN_STATE_NAME",
            "DEST_AIRPORT_IATA_CODE",
            "DEST_CITY_NAME",
            "DEST_STATE_NAME",
            "DEP_DELAY",
            "ARR_DELAY",
            "AIR_TIME",
            "DISTANCE",
            "OCCUPANCY_RATE",
        ]
    ]

    tickets = tickets.rename(
        columns={
            "ORIGIN_AIRPORT_IATA": "ORIGIN_AIRPORT_IATA_CODE",
            "DEST_AIRPORT_IATA": "DEST_AIRPORT_IATA_CODE",
            "REPORTING_CARRIER": "OP_CARRIER",
        }
    )

    tickets = tickets[
        [
            "ITIN_ID",
            "OP_CARRIER",
            "ORIGIN_AIRPORT_IATA_CODE",
            "ORIGIN_COUNTRY",
            "ORIGIN_STATE_ABR",
            "ORIGIN_STATE_NM",
            "DEST_AIRPORT_IATA_CODE",
            "PASSENGERS",
            "ITIN_FARE",
            "ONE_PASSENGERS_FARE",
        ]
    ]

    ## query out rows that does not related to the airport codes
    ## this action will cause several data point to lose
    flights = flights[
        (flights["ORIGIN_AIRPORT_IATA_CODE"].isin(airport_codes["AIRPORT_IATA_CODE"]))
        & (flights["DEST_AIRPORT_IATA_CODE"].isin(airport_codes["AIRPORT_IATA_CODE"]))
    ]

    tickets = tickets[
        (tickets["ORIGIN_AIRPORT_IATA_CODE"].isin(airport_codes["AIRPORT_IATA_CODE"]))
        & (tickets["DEST_AIRPORT_IATA_CODE"].isin(airport_codes["AIRPORT_IATA_CODE"]))
    ]

    return flights, tickets, airport_codes


def cleaning_data(
    flights: pd.DataFrame, tickets: pd.DataFrame, airport_codes: pd.DataFrame
) -> tuple:

    standard_flights, standard_tickets, standard_airports = standardlize_datasets(
        preprocess_flights(flights),
        preprocess_tickets(tickets),
        preprocess_airport_codes(airport_codes),
    )

    return (
        standard_flights.dropna(),
        standard_tickets.dropna(),
        standard_airports.dropna(),
    )


def main():
    ## load data
    flights = pd.read_csv("data/original_data/Flights.csv")
    tickets = pd.read_csv("data/original_data/Tickets.csv")
    airports = pd.read_csv("data/original_data/Airport_Codes.csv")

    ## standardlize the datasets
    flights, tickets, airports = cleaning_data(flights, tickets, airports)

    ## export data
    flights.to_csv("data/cleaned_data/Flights.csv", index=False)
    tickets.to_csv("data/cleaned_data/Tickets.csv", index=False)
    airports.to_csv("data/cleaned_data/Airport_Codes.csv", index=False)


if __name__ == "__main__":
    main()
