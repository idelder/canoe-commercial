"""
Sets up configuration for buildings sector aggregation
Written by Ian David Elder for the CANOE model
"""

import os
import pandas as pd
import yaml
import requests
import urllib.request
import zipfile
import sqlite3



def instantiate_database():
    
    # Check if database exists or needs to be built
    build_db = not os.path.exists(config.database_file)

    # Connect to the new database file
    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db

    # Build the database if it doesn't exist. Otherwise clear all data if forced
    if build_db: curs.executescript(open(config.schema_file, 'r').read())
    elif config.params['force_wipe_database']:
        tables = [t[0] for t in curs.execute("""SELECT name FROM sqlite_master WHERE type='table';""").fetchall()]
        for table in tables: curs.execute(f"DELETE FROM '{table}'")
        print("Database wiped prior to aggregation. See params.\n")
    
    conn.commit()

    # VACUUM operation to clean up any empty rows
    curs.execute("VACUUM;")
    conn.commit()

    conn.close()



class config:

    # File locations
    _this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
    input_files = _this_dir + 'input_files/'
    cache_dir = _this_dir + "data_cache/"

    if not os.path.exists(cache_dir): os.mkdir(cache_dir)

    tech_vints = {}
    lifetimes = {}

    _instance = None # singleton pattern


    def __new__(cls, *args, **kwargs):

        if isinstance(cls._instance, cls): return cls._instance
        cls._instance = super(config, cls).__new__(cls, *args, **kwargs)

        cls._get_params(cls._instance)
        cls._get_files(cls._instance)
        cls._get_aeo_data(cls._instance)
        cls._get_population_projections(cls._instance)
        cls._get_gdp_projections(cls._instance)

        print('Instantiated setup config.\n')

        return cls._instance


    def _get_params(cls):
        
        stream = open(config.input_files + "params.yaml", 'r')
        config.params = dict(yaml.load(stream, Loader=yaml.Loader))

        config.new_techs = pd.read_csv(config.input_files + 'new_technologies.csv', index_col=0)
        config.existing_techs = pd.read_csv(config.input_files + 'existing_technologies.csv', index_col=0)
        config.import_techs = pd.read_csv(config.input_files + 'import_technologies.csv', index_col=0)
        config.regions = pd.read_csv(config.input_files + 'regions.csv', index_col=0)
        config.fuel_commodities = pd.read_csv(config.input_files + 'fuel_commodities.csv', index_col=0)
        config.end_use_demands = pd.read_csv(config.input_files + 'end_use_demands.csv', index_col=0)
        config.time = pd.read_csv(config.input_files + 'time.csv', index_col=0)

        config.new_techs = config.new_techs.loc[config.new_techs['include_new']]
        config.all_techs = [*config.new_techs.index.values, *config.existing_techs.index.values]

        # Included regions and future periods
        config.model_periods = list(config.params['model_periods'])
        config.model_periods.sort()
        config.model_regions = config.regions.loc[(config.regions['include'])].index.unique().to_list()
        config.model_regions.sort()



    def _get_files(cls):

        config.schema_file = config.input_files + config.params['sqlite_schema']
        config.database_file = config._this_dir + config.params['sqlite_database']
        config.excel_template_file = config.input_files + config.params['excel_template']
        config.excel_target_file = config._this_dir + config.params['excel_output']



    def _get_aeo_data(cls):

        config.aeo_cdm = pd.read_excel(config.input_files + 'AEO2023_Reference_case_CDM_technolgy_menu_ktekx.xlsx',
                                             sheet_name='ktek', skiprows=68, index_col=False).iloc[1:,0:27]
        
        # Rename integer indexing to readable values to improve code readability and minimise bugs
        cdm_idx = pd.read_csv(config.input_files + 'aeo_cdm_indexing.csv', index_col=0)
        for col in config.aeo_cdm.columns:
            if col in cdm_idx.columns: config.aeo_cdm[col] = config.aeo_cdm[col].map(lambda n: cdm_idx.loc[n, col])
        

    
    def _get_population_projections(cls) -> pd.DataFrame:

        config.populations = dict()

        # Get historical population data from Statcan and take Q1
        df_exs = config._get_statcan_table(17100009, usecols=[0,1,9])
        df_exs = df_exs.loc[df_exs['REF_DATE'].str.contains('-01')]
        df_exs['REF_DATE'] = df_exs['REF_DATE'].str.removesuffix("-01")

        # Get projected population data from Statcan for M1 scenario
        df_proj = config._get_statcan_table(17100057, usecols=[0,1,3,4,5,12])
        df_proj['VALUE'] *= 1000
        df_proj = df_proj.loc[
            (df_proj['Projection scenario'] == 'Projection scenario M1: medium-growth') & 
            (df_proj['Sex'] == 'Both sexes') &
            (df_proj['Age group'] == 'All ages')]

        # For each region, take historical first, then provincial, then index to Canadian when that runs out
        for region, row in config.regions.iterrows():

            if not row ['include']: continue
            
            # Existing data
            exs = df_exs.loc[df_exs['GEO'].str.upper() == row['description'].upper()].dropna()

            # Projected provincial data
            prov = df_proj.loc[(df_proj['GEO'].str.upper() == row['description'].upper()) &
                            (df_proj['REF_DATE'] > int(exs['REF_DATE'].values[-1]))].dropna()
            
            # Index missing provincial data to Canadian projections
            ca = df_proj.loc[(df_proj['GEO'].str.upper() == 'CANADA') & 
                            (df_proj['REF_DATE'] >= int(prov['REF_DATE'].values[-1]))].dropna()
            ca['VALUE'] = ca['VALUE'].iloc[1::] * prov['VALUE'].values[-1] / ca['VALUE'].values[0]
            ca.dropna(inplace=True)

            # Create dataframe of population for all years
            data = [*exs['VALUE'].to_list(), *prov['VALUE'].to_list(), *ca['VALUE'].to_list()]
            pop = pd.DataFrame(index = range(int(exs['REF_DATE'].values[0]), int(ca['REF_DATE'].values[-1]+1)), data = [int(d) for d in data], columns=['population'])
            pop.index.rename('year', inplace=True)

            # Add to dictionary of regional population projections
            config.populations[region] = pop


    
    def _get_gdp_projections(cls) -> pd.DataFrame:

        config.gdp_index = dict()

        file = 'gdp_projections.csv'
        if os.path.isfile(config.cache_dir + file):
            df_gdp = pd.read_csv(config.cache_dir + file, index_col=0)
            print(f"Got {file} from local cache.")
        else: 
            df_gdp = pd.read_csv(config.params['gdp_url'])
            print(f"Downloading {file}...")

            # Filter and rename columns
            df_gdp = df_gdp.loc[(df_gdp['Variable'] == 'Real Gross Domestic Product ($2012 Millions)') & (df_gdp['Scenario'] == 'Evolving Policies')]
            df_gdp = df_gdp[['Year','Region','Value']].rename({"Year": "year", "Region": "region", "Value": "gdp"}, axis='columns')
        
            df_gdp.to_csv(config.cache_dir + file)
            print(f"Cached {file} locally.")

        # Index GDP to base year GDP by region
        df_gdp.set_index(['region','year'], inplace=True)
        df_gdp['gdp'] = df_gdp.index.map(lambda ry: df_gdp.loc[ry, 'gdp'] / df_gdp.loc[(ry[0], config.params['base_year']), 'gdp'])
        df_gdp = df_gdp.reset_index()

        # Create a dictionary of GDP indices
        for region, row in config.regions.iterrows():
            config.gdp_index[region] = df_gdp.loc[df_gdp['region'].str.lower() == row['description']].set_index('year')['gdp']

        

    # Have to put this here or it's awkward circular imports with utils
    def _get_statcan_table(table, save_as=None, **kwargs):
        
        if save_as == None: save_as = f"statcan_{table}.csv"
        if os.path.splitext(save_as)[1] != ".csv": save_as += ".csv"

        if not config.params['force_download']  and os.path.isfile(config.cache_dir + save_as):

            try:

                df = pd.read_csv(config.cache_dir + save_as, index_col=0)
                
                print(f"Got Statcan table {table} from local cache.")
                return df
            
            except Exception as e:

                print(f"Could not get Statcan table {table} from local cache. Trying to download instead.")

        # Make a request from the API for the table, returns response status and url for download
        url = f"https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/{table}/en"
        response = requests.get(url).json()

        # If successful, download the table
        if response['status'] == 'SUCCESS':

            print(f"Downloading Statcan table {table}...")

            # Download and open the zip file
            filehandle,_ = urllib.request.urlretrieve(response['object'])
            zip_file_object = zipfile.ZipFile(filehandle, 'r')

            # Read the table from inside the zip file
            from_file = zip_file_object.open(f"{table}.csv", "r")
            df = pd.read_csv(from_file, **kwargs)
            from_file.close()

            df.to_csv(config.cache_dir + save_as)

            print(f"Cached Statcan table {table}.")
            return df

        else:

            print(f"Request for {table} from Statcan failed. Status: {response['status']}")
            return None
        


# Instantiate on import
config()
