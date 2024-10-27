# the core running file that runs everything
from data_preprocessing import *
from finding_roundtripFlights import *
from finding_ticketPrice import *
from finding_cost_revenue import *
from checking_data_contrain import *


def main():
    uncleaned_airport_code = pd.read_csv("data/original_data/Airport_Codes.csv")
    uncleaned_flights = pd.read_csv("data/original_data/Flights.csv")
    uncleaned_tickets = pd.read_csv("data/original_data/Tickets.csv")

    ## data cleaning
    flights, tickets, airport_codes = cleaning_data(
        uncleaned_flights, uncleaned_tickets, uncleaned_airport_code
    )

    ## adding contrain check for all three dataframes
    check_flights_constrains(flights)
    check_tickets_constrains(tickets)
    check_airport_codes_constrains(airport_codes)

    ## finding the average ticket price of round trip tickets
    average_TicketPrice = finding_avg_roundtripTickets(tickets)

    ## finding round_trip routes
    roundTrip_flights, soleTrip_flights = finding_roundtripFlights(flights)

    ## finding cost and revenue for each round trip routes
    roundtrip_profit = finding_cost_revenue(
        roundTrip_flights, average_TicketPrice, airport_codes
    )

    ### temporary data saving - outlierHandle
    (
        roundTrip_flights.to_csv(
            "data/temproary_data/round_trip_flights.csv", index=False
        )
        if isinstance(roundTrip_flights, pd.DataFrame)
        else None
    )
    (
        soleTrip_flights.to_csv(
            "data/temproary_data/sole_trip_flights.csv", index=False
        )
        if isinstance(soleTrip_flights, pd.DataFrame)
        else None
    )
    (
        average_TicketPrice.to_csv(
            "data/temproary_data/average_ticket_price.csv", index=False
        )
        if isinstance(average_TicketPrice, pd.DataFrame)
        else None
    )
    (
        roundtrip_profit.to_csv("data/temproary_data/roundTrip_profit.csv", index=False)
        if isinstance(roundtrip_profit, pd.DataFrame)
        else None
    )


if __name__ == "__main__":
    main()
