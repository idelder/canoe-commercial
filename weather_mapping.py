import pandas as pd
import numpy as np
import utils
import os
import requests
from io import StringIO
from datetime import datetime
from setup import config

weather_maps = dict() # Maps that have already been loaded weather_maps[region] = 8760x8760 np array

# Weather data
initialised = False
df_us_tmp: pd.DataFrame = None
df_us_hum: pd.DataFrame = None
df_ca_tmp: pd.DataFrame = None
df_ca_hum: pd.DataFrame = None



# Downloads temperature and humidity data from Renewables Ninja, but only caches weather-year data
def get_weather_data(url: str) -> pd.DataFrame:

    file_name = os.path.splitext(url.split("/")[-1].split("\\")[-1])[0] + f"_{config.params['weather_year']-1}-{config.params['weather_year']+1}.csv"

    # Get from local cache if it exists
    if os.path.isfile(config.cache_dir + file_name):

        print(f"Got {file_name} from local cache.")

        df = pd.read_csv(config.cache_dir + file_name, index_col=0)
        df.index = pd.to_datetime(df.index)

    else:

        print(f"Downloading {file_name} from Renewables Ninja API...")

        # Handle downloading data from Renewables Ninja API
        s = requests.session()
        s.headers = {'Authorization': 'Token ' + config.params['weather']['api_token']} # attach API token
        r = s.get(url, params={'format': 'json'})
        data = StringIO(r.text)
        df = pd.read_csv(data, skiprows=3, index_col=0)

        # Convert index to datetime index
        df.index = pd.to_datetime(df.index)

        # Filter to weather year data
        df: pd.DataFrame = df.loc[(config.params['weather_year'] - 1 <= df.index.year) & (df.index.year <= config.params['weather_year'] + 1)]

        # Cache dataframe locally as a csv
        df.to_csv(config.cache_dir + file_name)

    # Data is originally in UTC timezone so convert to model timezone
    df.index = df.index.tz_localize('UTC')
    df.index = df.index.tz_convert(config.params['timezone'])

    # Filter to only 8760 of weather year
    df = df.loc[df.index.year == config.params['weather_year']]

    return df



def initialise_weather_data():

    global initialised, df_us_tmp, df_us_hum, df_ca_tmp, df_ca_hum

    if initialised: return

    # Get hourly weather data from Renewables Ninja
    df_us_tmp = get_weather_data(config.params['weather']['us_temperature_url'])
    df_us_hum = get_weather_data(config.params['weather']['us_humidity_url'])
    df_ca_tmp = get_weather_data(config.params['weather']['ca_temperature_url'])
    df_ca_hum = get_weather_data(config.params['weather']['ca_humidity_url'])

    initialised = True



def map_data(region: str, us_data: np.ndarray) -> tuple[pd.Series, np.ndarray]:

    reg_config = config.regions.loc[region]
    map_file = f"weather_map_{reg_config['us_state']}-{region}_{str(config.params['weather_year'])}_{config.params['timezone']}.npz"
    
    # If the mapper already exists then just use it
    # Already loaded
    if region in weather_maps.keys(): return apply_map(region, us_data)
    # Load from local cache
    elif not config.params['force_generate_weather_maps'] and os.path.isfile(config.cache_dir + map_file):
        print(f"Loading weather map {map_file} from local cache...")
        with open(config.cache_dir + map_file, 'rb') as file:
            weather_maps[region] = np.load(file)['arr_0']
        try:
            return apply_map(region, us_data)
        except Exception as e: # if failed regenerate the map
            print(f"Failed to apply weather map from local cache. Regenerating. Error:\n{e}")
    
    ## Otherwise generate the map
    print(f"\nGenerating a weather-based data map from {reg_config['us_state']} to {region}...")

    # A 2D matrix map of which US data points to use per Canadian datum
    weather_maps[region] = np.zeros((8760,8760))

    # Get temperature and humidity data ready
    initialise_weather_data()

    # Get hourly temperature and humidity for this region
    df_ca: pd.DataFrame = pd.concat([df_ca_tmp[reg_config['ca_rninja']], df_ca_hum[reg_config['ca_rninja']]], axis=1).astype(float)
    df_us: pd.DataFrame = pd.concat([df_us_tmp[f"US.{reg_config['us_state']}"], df_us_hum[f"US.{reg_config['us_state']}"]], axis=1).astype(float)
    df_ca.columns = ['temp','hum']
    df_us.columns = ['temp','hum']

    unmatched = 0.0
    for h in range(8760):

        ca_row = df_ca.iloc[h]

        # For this datum, boolean vector of relevant data in US data vector
        row_map = 1.0*np.array((ca_row['temp'] <= df_us['temp']+1) &
            (ca_row['temp'] >= df_us['temp']-1) &
            (ca_row['hum'] == df_us['hum'])).transpose()
        
        # If no match, maybe Canadian temps are hotter or (more likely) colder than all us temps
        if np.sum(row_map) == 0: # Did not find a match
            unmatched += 1

            # Hotter than anything in US record so take hottest US hour
            if ca_row['temp'] > np.max(df_us['temp']):
                row_map = 1.0*np.array(df_us['temp'] == np.max(df_us['temp'])).transpose()

            # Colder than anything in US record so take coldest US hour 
            elif ca_row['temp'] < np.min(df_us['temp']):
                row_map = 1.0*np.array(df_us['temp'] == np.min(df_us['temp'])).transpose()
        
        # Get the mean of relevant US data and or set NaN if no mappable hours -> will be interpolated
        row_map *= np.nan if np.sum(row_map) == 0 else 1 / np.sum(row_map)

        # Save these matrix maps per region to save having to repeat the above slow process
        weather_maps[region][h,:] = row_map.copy()

    print(f"{round((1-unmatched/8760)*100, 1)}% of hours found +-1C temperature match.")

    # Cache the generated weather map locally
    #np.savetxt(config.cache_dir + map_file, weather_maps[region], delimiter=',')
    with open(config.cache_dir + map_file, 'wb') as file:
        np.savez_compressed(file, weather_maps[region])
    print(f"Weather map generated and cached as {map_file}")

    return apply_map(region, us_data)



def apply_map(region: str, us_data: np.ndarray) -> tuple[pd.Series, np.ndarray]:

    print(f"Applying weather map for {region}...")

    # Canadian data starts as us data mapped to temperature and dew point temperature
    ca_data = pd.Series(np.matmul(weather_maps[region], us_data)).interpolate(method='linear')

    # Apply time of week multipliers
    #ca_data *= tow_mults
    
    # Return mapped data and time-of-week multipliers Mon -> Sun
    return ca_data



def get_weekly_variation(data: np.ndarray):

    # Then get the day of week of Jan 1 for each year. Monday is 0, Sunday 6
    jan_1_us = datetime.weekday(datetime.fromisoformat(f"{config.params['weather_year']}-01-01"))
    jan_1_ca = datetime.weekday(datetime.fromisoformat(f"{config.params['weather_year']}-01-01"))

    # Get multipliers for time of the week, hourly -> this doesnt work as temperature effects are double counted
    daily_avg = np.array([np.mean(data[24*d:24*d+23]) for d in range(364)])
    weekly_avg = np.array([np.mean(data[7*24*w:7*24*w+7*24-1]) for w in range(52)])
    day_of_week = [np.mean(daily_avg[d:52*7:7]/weekly_avg) for d in range(7)]
    hour_of_day = [np.mean(data[h:24*7*52:24]/daily_avg) for h in range(24)]
    time_of_week = [day_of_week[h//24] * hour_of_day[h%24] for h in range(24*7)] # option a. hour of day factor times day of week factor. Use this
    #time_of_week = [np.mean(data[h:24*7*52:24*7]/weekly_avg) for h in range(24*7)] # option b. hour of week factor

    # Shift multipliers to correct day of week based on jan 1 day
    time_of_week_zeroed = time_of_week[-24*jan_1_us::] + time_of_week[0:-24*jan_1_us] # starts on monday
    tow_mults = time_of_week_zeroed[24*jan_1_ca::] + time_of_week_zeroed[0:24*jan_1_ca] # starts on jan 1 day base year
    
    # Take hourly time of week multiplier and stretch out to whole year (8760)
    tow_mults = tow_mults*52 + tow_mults[0:24] # 52 weeks + 1 day in a year
    tow_mults /= np.mean(tow_mults) # normalise

    return time_of_week_zeroed



if __name__ == "__main__":

    initialise_weather_data()