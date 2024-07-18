# WeatherPyFile
This python file is part of the capstone project (https://github.com/DhavalDep/dd_capstone_weather_streamlit), where a UK Weather Dashboard was created.

The purpose of this code is to collect neccesary data from WeatherAPI and store this in a Database. 
Two tables were created (current weather and forecasted weather) these SQL tables is where the data was stored.

This python file is run on a CRON job hourly to collect real time information which is used by a streamlit dashboard
to present the weather information of a chosen UK city to the user tidily.
