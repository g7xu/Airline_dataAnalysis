import pandas as pd
import numpy as np
import re


def str_to_float(text):
    """
    convert a string to a float. if the input is Two, convert it to 2.0
    ---
    text: str to be converted
    ---
    return: converted float or np.nan if conversion fails
    >>> str_to_float("1.0")
    1.0
    >>> str_to_float("1")
    1.0
    >>> str_to_float("1.0.0")
    nan
    >>> str_to_float("Two")
    2.0
    >>> str_to_float("-1.0")
    1.0
    >>> str_to_float("1.0-")
    nan
    >>> str_to_float("20000.00")
    20000.0
    """
    try:
        return abs(float(text))
    except:
        if text == "Two":
            return 2.0
        return np.nan


def main():
    # loading data
    flights = pd.read_csv("data/original_data/Flights.csv")
    tickets = pd.read_csv("data/original_data/Tickets.csv")
    airport_codes = pd.read_csv("data/original_data/Airport_Codes.csv")

    # cleaning flights data
    flights["FL_DATE"] = pd.to_datetime(flights["FL_DATE"])

    # finding the specific city and state name
    split_ORIGIN_CITY_STATE = flights["ORIGIN_CITY_NAME"].str.split(", ")
    split_DEST_CITY_STATE = flights["DEST_CITY_NAME"].str.split(", ")

    flights["ORIGIN_CITY_NAME"] = split_ORIGIN_CITY_STATE.str[0]
    flights["ORIGIN_STATE_NAME"] = split_ORIGIN_CITY_STATE.str[1]

    flights["DEST_STATE_NAME"] = split_DEST_CITY_STATE.str[1]
    flights["DEST_CITY_NAME"] = split_DEST_CITY_STATE.str[0]

    flights["DISTANCE"] = flights["DISTANCE"].apply(str_to_float)

    flights.to_csv("data/cleaned_data/flights.csv", index=False)


if __name__ == "__main__":
    main()
