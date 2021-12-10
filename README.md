## Setup

1. Install make: `pip3 install make`
2. Setup: `make setup` 
3. Put your credentials in the `.env` file: `FTX_API_KEY`, `FTX_API_SECRET`
4. Run the program: `make run`

### Running multiple markets: `python main.py FTX:BTC-PERP,ETH-PERP`

### Running multiple exchanges: `python main.py "FTX:BTC-PERP,ETH-PERP; ROBIN:ABC-PERP"` 
be careful about the quote marks





## Assignment (for future reference)

### Create SQL tables to store the following data:

* Trade executions (assume multiple trading pairs and exchanges) Trade execution example
* Aggregated historical trade data, “candles” (assume multiple trading pairs, exchanges and intervals) Candle example

###Write a solution that accomplishes the following:
* Fetches historical data for the BTC-PERP pair from FTX’s REST API for the following intervals:
  - 1 minute
  - 1 hour
  - 1 day
  
* Saves historical data across the three intervals to the tables outlined above.
* Updates the most recent candle (most recent minute, hour and day) with trade executions provided by FTX’s Websocket stream. In other words, the historical values obtained via the REST API should be updated after they have been collected with real-time data published over the Websocket. At the turn of the most recent period, the intervals should match what is subsequently provided via the REST API - If not, then log the deltas (the difference between your own candle and the one provided by the exchange).
  
###Bonus
 * Record open interest at close of each interval.
 * Resources
