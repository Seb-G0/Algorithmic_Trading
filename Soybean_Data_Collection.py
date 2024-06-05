#Soybean Data Collection
#May 2024
#Sebastian Gonzalez and Zain Zaidi


"""This will be one of three files submitted for the May term of Sebastian and Zain.
This file will be focussed on the data collection and building of CSV files.
Each County that grows Soybeans will have a CSV where data will be stored in the following order:
[Year, Yield, Q2, Q3] The name of the file will be State_County.csv

This data will be collected from OpenMeteo and NASS(National Agricultural Statistics Service)
"""

#imports
import pandas as pd
import json
import openmeteo_requests
from retry_requests import retry
import requests_cache
from itertools import islice


#Longitude and Latitude for each County
"""In order to access the data from OpenMeteo, longitude and latitude are required, therefore for each county,
we must get that data.
Luckily this data has already been compiled: https://simplemaps.com/data/us-counties
It can be found in the github as uscounties.csv

We are only getting data for counties already in AcresPlanted.csv as those are the only counties that plant soybeans"""

countyLocations = dict()

def getLocation():
    """The Longitude and Latitude data is stored in a dictionary as there is no need to make
    another file just to hold this when it is in a csv"""

    AcresPlanted = pd.read_csv("AcresPlanted.csv") #Reads CSV downloaded from NASS
    AcresPlanted2023 = AcresPlanted[AcresPlanted['Year'] == 2023] #Filters to 2023
    Counties = AcresPlanted2023[['State', 'County']] #Gets all counties mentioned in 2023

    """Now we must collect the Latitude and Longitude from uscounties.csv
    unfortunately the two csv files don't use the same spelling for counties, which is annoying so we have to do some magic"""

    uscounties = pd.read_csv("uscounties.csv")
    uscounties['county'] = uscounties['county'].str.lower().str.replace(' ', '', regex=True)
    for index, row in Counties.iterrows():
        state, county = row['State'].title(), (row['County'].lower()).replace(' ', '')
        countySpecific = uscounties[(uscounties['state_name'] == state) & (uscounties['county'] == county)]
        Latitude, Longitude = countySpecific['lat'].values[0], countySpecific['lng'].values[0]
        countyLocations[state + '_' + county] = [Latitude, Longitude]

"""This only needs to be run once as the dictionary is stored with JSON"""
# getLocation()
# with open('countyLocations.json', 'w') as file:
#     json.dump(countyLocations, file)

with open('countyLocations.json', 'r') as file:
    countyLocations = json.load(file)

"""Now we are going to build the CSV files. Each CSV file will be titled the same way as the dictionary"""

def buildCSV():
    Yield = pd.read_csv("Yield(2023-2000).csv")
    Yield['County'] = Yield['County'].str.lower().str.replace(' ', '', regex=True)
    for name, location in countyLocations.items():
        state, county = name.split("_")
        state = state.upper()
        """Yield comes from NASS and goes from 2000 - 2023"""
        County_Yield = Yield[(Yield['State'] == state) & (Yield['County'] == county)]
        if County_Yield.empty:
            continue
        """Unfortunately, the NASS did not record certain years, so some CSV files will be shorter than others now it is time for OpenMeteo"""
        County_Yield = County_Yield[['Year', 'Value']] #Filter so Year and Yield are only recorded

        """All the data from OpenMeteo for each county has already been stored in csvfiles named "RAW_State_County.csv" 
        That code is written in the OpenMeteoBuilder method now we will process each file"""

        #Each county will have 3 final csvs, a weekly, monthly, and quaterly
        RAW_DATA = pd.read_csv(f"RAW_County_CSV/RAW_{state.title()}_{county}.csv")
        print(name)
        for Type in ['quaterly', 'monthly', 'weekly']:
            data = County_Yield


            if Type == 'weekly':
                ROWS = []
                COLUMNS = ["Year", "Yield"]
                """Now we create new columns"""
                for w in range(0, 53):
                    for t in ["temperature_2m_mean", "precipitation_sum", "sunshine_duration"]:
                        for m in ['mean', 'std', 'min', 'max']:
                            COLUMN_NAME = f"w{w}_{t}_{m}"
                            COLUMNS.append(COLUMN_NAME)

                RAW_DATA['date'] = RAW_DATA['date'].str[0:4]
                for year in data['Year']:
                    yearly_yield = data[data['Year'] == year]['Value'].values[0]
                    row = [year, yearly_yield]
                    year = str(year)
                    YEARLY = RAW_DATA[RAW_DATA['date'] == year]
                    YEARLY.reset_index()
                    for i in range(0, 365, 7):
                        sI = i
                        eI = i + 7
                        WEEKLY_DATA = YEARLY.iloc[sI:eI]
                        categories = process(WEEKLY_DATA, ["temperature_2m_mean", "precipitation_sum", "sunshine_duration"])
                        row += categories
                    ROWS.append(row)
                newCSV = pd.DataFrame(columns=COLUMNS, data=ROWS)
                newCSV.to_csv(f"WEEKLY_{state}_{county}.csv", index=False)

            if Type == 'monthly':
                ROWS = []
                COLUMNS = ["Year", "Yield"]
                """Now we create new columns"""
                for w in range(0, 12):
                    for t in ["temperature_2m_mean", "precipitation_sum", "sunshine_duration"]:
                        for m in ['mean', 'std', 'min', 'max']:
                            COLUMN_NAME = f"m{w}_{t}_{m}"
                            COLUMNS.append(COLUMN_NAME)
                RAW_DATA['date'] = RAW_DATA['date'].str[0:7]
                for year in data['Year']:
                    yearly_yield = data[data['Year'] == year]['Value'].values[0]
                    row = [year, yearly_yield]
                    year = str(year)
                    for m in range(1, 13):
                        if m < 10:
                            m = '0' + str(m)
                        f = year + '-' + str(m)
                        MONTHLY = RAW_DATA[RAW_DATA['date'] == f]
                        categories = process(MONTHLY, ["temperature_2m_mean", "precipitation_sum", "sunshine_duration"])
                        row += categories
                    ROWS.append(row)
                newCSV = pd.DataFrame(columns=COLUMNS, data=ROWS)
                newCSV.to_csv(f"MONTHLY_{state}_{county}.csv", index=False)


            if Type == 'quaterly':
                ROWS = []
                COLUMNS = ["Year", "Yield"]
                """Now we create new columns"""

                for w in range(1, 5):
                    for t in ["temperature_2m_mean", "precipitation_sum", "sunshine_duration"]:
                        for m in ['mean', 'std', 'min', 'max']:
                            COLUMN_NAME = f"q{w}_{t}_{m}"
                            COLUMNS.append(COLUMN_NAME)

                RAW_DATA['date'] = RAW_DATA['date'].str[0:7]
                for year in data['Year']:
                    yearly_yield = data[data['Year'] == year]['Value'].values[0]
                    row = [year, yearly_yield]
                    year = str(year)

                    for q in [['01', '02', '03'], ['04', '05', '06'], ['07', '08', '09'], ['10', '11', '12']]:
                        f1 = year + '-' + q[0]
                        f2 = year + '-' + q[1]
                        f3 = year + '-' + q[2]
                        QUATERLY = RAW_DATA[(RAW_DATA['date'] == f1) | (RAW_DATA['date'] == f2) | (RAW_DATA['date'] == f3)]
                        categories = process(QUATERLY, ["temperature_2m_mean", "precipitation_sum", "sunshine_duration"])
                        row += categories
                    ROWS.append(row)
                newCSV = pd.DataFrame(columns=COLUMNS, data=ROWS)
                newCSV.to_csv(f"QUATERLY_{state}_{county}.csv", index=False)
def process(data, variables):
    """Helper Function to create CSVs"""
    returnVal = []
    for i in range(3):
        returnVal.append(data.get(variables[i]).mean())
        returnVal.append(data.get(variables[i]).std())
        returnVal.append(data.get(variables[i]).min())
        returnVal.append(data.get(variables[i]).max())
    return returnVal

def chunks(data):
    """Helper Function to split countyLocations for easy Batching"""
    SIZE = len(data.keys())//10
    it = iter(data)
    for i in range(0, len(data), SIZE):
        yield {k:data[k] for k in islice(it, SIZE)}


def OpenMeteoBuilder():
    """This Method creates the Open Meteo CSVs which will be combined later with the yields csv to create a csv for each county"""

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://customer-archive-api.open-meteo.com/v1/archive"

    #calls chunks to break up the calls
    for batch in chunks(countyLocations):
        latitude = [cords[0] for cords in batch.values()]
        longitude = [cords[1] for cords in batch.values()]
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": "2000-01-01",
            "end_date": "2023-12-31",
            "daily": ["temperature_2m_mean", "sunshine_duration", "precipitation_sum"],
            "apikey": "XXXXXXXXXXXXX" #No free uses for you
        }
        responses = openmeteo.weather_api(url, params=params)
        """This data still needs to be processed before being stored in OpenMeteo.CSV"""
        for name, response in zip(batch.keys(), responses):
            daily = response.Daily()
            daily_temperature_2m_mean = daily.Variables(0).ValuesAsNumpy()
            daily_sunshine_duration = daily.Variables(1).ValuesAsNumpy()
            daily_precipitation_sum = daily.Variables(2).ValuesAsNumpy()
            daily_data = {"date": pd.date_range(
                start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=daily.Interval()),
                inclusive="left")}
            daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
            daily_data["sunshine_duration"] = daily_sunshine_duration
            daily_data["precipitation_sum"] = daily_precipitation_sum

            daily_dataframe = pd.DataFrame(data=daily_data)
            daily_dataframe.to_csv(f"RAW_{name}.csv")


buildCSV()

"""Now that we collected the RAW data already, all we have to do is process it
To make sure we have as many options as possible, we will process one file for 52 weeks, one for 12 months, and one for 4 quarters"""

# OpenMeteoBuilder()




