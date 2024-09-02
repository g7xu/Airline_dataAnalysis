# the preprocessing script won't remove missing value
# it will
# 1. remove all the cancelled flights
# 2. only look at the round trip tickets
# 3. constrain the range of certain column (ex. distance can not be negative)
# 4. convert the data type of certain column (ex. convert string to float)
# 5. adjust the column name to be more appropriate
import pandas as pd
import numpy as np
import re


def str_to_float(text):
    try:
        return abs(float(text))
    except:
        if text == "Two":
            return 2.0
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


def main():
    # loading flights data
    flights = pd.read_csv("data/original_data/Flights.csv")

    ## only look at flights that is not cancelled
    flights = flights[flights["CANCELLED"] == 0]

    ## adjusting data type of certain columns
    flights["FL_DATE"] = pd.to_datetime(flights["FL_DATE"])
    flights["OP_CARRIER"] = flights["OP_CARRIER"].astype(str)
    flights["AIR_TIME"] = flights["AIR_TIME"].apply(str_to_float)
    flights["DISTANCE"] = flights["DISTANCE"].apply(str_to_float)

    ## split certain column into two
    split_ORIGIN_CITY_STATE = flights["ORIGIN_CITY_NAME"].str.split(", ")
    split_DEST_CITY_STATE = flights["DEST_CITY_NAME"].str.split(", ")

    flights["ORIGIN_CITY_NAME"] = split_ORIGIN_CITY_STATE.str[0]
    flights["ORIGIN_STATE_NAME"] = split_ORIGIN_CITY_STATE.str[1]

    flights["DEST_STATE_NAME"] = split_DEST_CITY_STATE.str[1]
    flights["DEST_CITY_NAME"] = split_DEST_CITY_STATE.str[0]

    ## adjusting column name
    flights.rename(columns={"OP_CARRIER": "OP_CARRIER_IATA_CODE"}, inplace=True)
    flights.rename(columns={"ORIGIN": "ORIGIN_AIRPORT_IATA"}, inplace=True)
    flights.rename(columns={"DESTINATION": "DEST_AIRPORT_IATA"}, inplace=True)

    ## dropping unnesserary columns
    flights.drop(
        columns=["ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID", "CANCELLED"], inplace=True
    )

    # loading tickets data
    tickets = pd.read_csv("data/original_data/Tickets.csv")

    ## only look at tickets that is round trip
    tickets = tickets[tickets["ROUNDTRIP"] == 1]

    ## adjusting data type of certain columns
    tickets["YEAR"] = tickets["YEAR"].astype(int)
    tickets["ITIN_FARE"] = tickets["ITIN_FARE"].apply(find_number)

    ## adjusting column name
    tickets.rename(columns={"ORIGIN": "ORIGIN_AIRPORT_IATA"}, inplace=True)
    tickets.rename(columns={"DESTINATION": "DEST_AIRPORT_IATA"}, inplace=True)

    ## dropping unnesserary columns
    tickets.drop(columns=["ORIGIN_COUNTRY", "ROUNDTRIP"], inplace=True)

    # loading airport codes data
    airport_codes = pd.read_csv("data/original_data/Airport_Codes.csv")

    ## ignore the airport that has no IATA code and not in US and only look at medium and large airport
    airport_codes = airport_codes[
        (
            # (airport_codes["TYPE"] == "small_airport")
            (airport_codes["TYPE"] == "medium_airport")
            | (airport_codes["TYPE"] == "large_airport")
        )
        & (airport_codes["ISO_COUNTRY"] == "US")
        & (airport_codes["IATA_CODE"].notnull())
    ]

    ## split oordinates into two column
    airport_codes["COORDINATES_LONGITUDE"] = (
        airport_codes["COORDINATES"].apply(lambda x: x.split(", ")[0]).astype(float)
    )
    airport_codes["COORDINATES_LATITUDE"] = (
        airport_codes["COORDINATES"].apply(lambda x: x.split(", ")[1]).astype(float)
    )

    # dropping unnesserary columns
    airport_codes.drop(
        columns=["COORDINATES", "CONTINENT", "ISO_COUNTRY"], inplace=True
    )

    # export cleaned data to csv in the data file
    flights.drop_duplicates().to_csv("data/cleaned_data/Flights.csv", index=False)
    tickets.drop_duplicates().to_csv("data/cleaned_data/Tickets.csv", index=False)
    airport_codes.drop_duplicates().to_csv(
        "data/cleaned_data/Airport_Codes.csv", index=False
    )


if __name__ == "__main__":
    main()
