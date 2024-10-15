# This file is used to check the data contrain of the dataframes that are used in the main_running.py file
# only check flights, tickets, and airport_codes dataframes
import pandas as pd
import numpy as np

AMERICA_LOWEST_ELEVATION_ft = -282
AMERICA_HIGHEST_ELEVATION_ft = 20310
AMERICA_CORDINATE_LONG_LAT_RANGE = [
    [(-126, -67.130), (23, 48.89)],  # America main land
    [(-188, -129), (47, 72)],  # Alaska
    [(-191, -146), (-6, 25)],  # other islands
]


def checking_long_lat(long: float, lat: float) -> bool:
    for std_long_lat in AMERICA_CORDINATE_LONG_LAT_RANGE:
        if (
            long >= std_long_lat[0][0]
            and long <= std_long_lat[0][1]
            and lat >= std_long_lat[1][0]
            and lat <= std_long_lat[1][1]
        ):
            return True
    return False


def check_flights_constrains(flights: pd.DataFrame) -> bool:

    # checking the FL_DATE is in the first quarter
    assert (
        flights["FL_DATE"].dt.month.isin([1, 2, 3]).all()
    ), "FL_DATE is not in the first quarter"

    # checking IATA code
    assert (
        (flights["ORIGIN_AIRPORT_IATA_CODE"].str.len() == 3)
        & (flights["DEST_AIRPORT_IATA_CODE"].str.len() == 3)
    ).all(), "IATA_CODE is not 3 characters long"

    # AIR_TIME should never be negative or 0
    assert (flights["AIR_TIME"].dropna() > 0).all(), "AIR_TIME is not positive"

    # Distance should never be negative or 0
    assert (flights["DISTANCE"].dropna() > 0).all(), "DISTANCE is not positive"

    # OCCUPANCY should between 0 and 1
    assert (
        flights["OCCUPANCY_RATE"].dropna().between(0, 1).all()
    ), "OCCUPANCY_RATE is not between 0 and 1"


def check_tickets_constrains(tickets: pd.DataFrame) -> bool:

    # checking IATA code
    assert (
        tickets["ORIGIN_AIRPORT_IATA_CODE"].str.len() == 3
    ).all(), "ORIGIN_AIRPORT_IATA_CODE is not 3 characters long"

    assert (
        tickets["DEST_AIRPORT_IATA_CODE"].str.len() == 3
    ).all(), "DEST_AIRPORT_IATA_CODE is not 3 characters long"

    assert (
        tickets["ORIGIN_AIRPORT_IATA_CODE"] != tickets["DEST_AIRPORT_IATA_CODE"]
    ).all(), (
        "ORIGIN_AIRPORT_IATA_CODE and DEST_AIRPORT_IATA_CODE should not be the same"
    )

    # passenger should be whole number and should never be lower than 0
    assert (
        (tickets["PASSENGERS"].apply(lambda x: x.is_integer()))
        | (tickets["PASSENGERS"].isna())
    ).all(), "PASSENGERS is not integer"
    assert (
        (tickets["PASSENGERS"].apply(lambda x: x > 0)) | (tickets["PASSENGERS"].isna())
    ).all(), "PASSENGERS COUNT should be postive"

    # ITIN_FARE should alway be positive
    assert (tickets["ITIN_FARE"].dropna() >= 0).all(), "ITIN_FARE is not positive"


def check_airport_codes_constrains(airport_codes: pd.DataFrame) -> bool:

    # checking airport code
    assert airport_codes[
        "AIRPORT_IATA_CODE"
    ].is_unique, "AIRPORT_IATA_CODE is not unique"
    assert (
        airport_codes["AIRPORT_IATA_CODE"].str.len() == 3
    ).all(), "AIRPORT_IATA_CODE is not 3 characters long"

    # data set should only contain medium and large airport
    assert (
        airport_codes["TYPE"].isin(["medium_airport", "large_airport"]).all()
    ), "AIRPORT_TYPE is not medium_airport or large_airport"

    # all airport has valid elevation level
    assert (
        airport_codes["ELEVATION_FT"] >= AMERICA_LOWEST_ELEVATION_ft
    ).all(), "AIRPORT_ELEVATION_ft is lower than the lowest elevation in America"
    assert (
        airport_codes["ELEVATION_FT"] <= AMERICA_HIGHEST_ELEVATION_ft
    ).all(), "AIRPORT_ELEVATION_ft is higher than the highest elevation in America"

    # all airport has valid coordinates
    assert airport_codes.apply(
        lambda row: checking_long_lat(
            row["COORDINATES_LONGITUDE"],
            row["COORDINATES_LATITUDE"],
        ),
        axis=1,
    ).all(), "COORDINATES_LONGITUDE and COORDINATES_LATITUDE is not in America"


def main():
    pass


if __name__ == "__main__":
    main()
