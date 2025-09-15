"""
Aggregates residential non-subsector-specific data
Written by Ian David Elder for the CANOE model
"""

from setup import config
import utils
import pandas as pd
import sqlite3
import os
import comstock_dsd
import existing_capacity
import new_capacity

# Shortens lines a bit
fuel_commodities = config.fuel_commodities
end_use_demands = config.end_use_demands
conversion_factors = config.params['conversion_factors']



def aggregate():

    pre_process()

    # Aggregate space heating and cooling
    for region in config.model_regions:
        
        print(f"Aggregating {region}...\n")

        df_dsd = comstock_dsd.calculate_dsds(region)
        df_exs = existing_capacity.aggregate_region(region, df_dsd)
        new_capacity.aggregate_region(region, df_exs) # existing data for annual capacity factors
        
        print(f"Aggregated {region}.\n")

    if config.params['include_emissions']: aggregate_emissions()
    if config.params['include_imports']: aggregate_imports()

    post_process()

    



# For non-regional aggregation
def pre_process():

    # Connect to the new database file
    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db


    """
    ##############################################################
        Basic parameters
    ##############################################################
    """

    for period in config.model_periods:
        for h, row in config.time.iterrows():
            curs.execute(
                f"""REPLACE INTO
                TimeSegmentFraction(season, tod, segfrac)
                VALUES('{row['season']}', '{row['tod']}', {1/8760})"""
            )

        for i, season in enumerate(config.time['season'].unique()):
            curs.execute(
                f"""REPLACE INTO
                TimeSeason(period, sequence, season)
                VALUES({period}, {i}, '{season}')"""
            )

    for season in config.time['season'].unique():
        curs.execute(
            f"""REPLACE INTO
            SeasonLabel(season)
            VALUES('{season}')"""
        )

    for i, tod in enumerate(config.time['tod'].unique()):
        curs.execute(
            f"""REPLACE INTO
            TimeOfDay(tod)
            VALUES('{tod}')"""
        )
        
    for i, period in enumerate([*config.model_periods, config.model_periods[-1] + config.params['period_step']]):
        curs.execute(
            f"""REPLACE INTO
            TimePeriod(sequence, period, flag)
            VALUES({i}, {period}, 'f')"""
        )

    for region, row in config.regions.iterrows():
        if row['include']:
            curs.execute(
                f"""REPLACE INTO
                Region(region, notes)
                VALUES('{region}', '{row['description']}')"""
            )


    """
    ##############################################################
        Commodities
    ##############################################################
    """
    
    for _code, comm_config in config.fuel_commodities.iterrows():
        curs.execute(
            f"""REPLACE INTO
            Commodity(name, flag, description, data_id)
            VALUES('{comm_config['comm']}', 'p', '({comm_config['unit']}) {comm_config['description']}', '{utils.data_id()}')"""
        )
        
    if config.params['include_emissions']:
        # CO2-equivalent emission commodity
        curs.execute(
            f"""REPLACE INTO
            Commodity(name, flag, description, data_id)
            VALUES('{config.params['emission_commodity']}', 'e', '(ktCO2eq) CO2-equivalent emissions', '{utils.data_id()}')"""
        )


    conn.commit()
    conn.close()

    print(f"Pre-aggregation complete.\n")
        


# For non-regional post-subsector aggregation
def post_process():

    # Connect to the new database file
    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db
    

    """
    ##############################################################
        Existing time periods
    ##############################################################
    """

    # Add all existing vintages to existing time periods
    exs_vints = set([fetch[0] for fetch in curs.execute(f"SELECT vintage FROM Efficiency").fetchall() if fetch[0] not in config.model_periods])

    for vint in exs_vints:
        curs.execute(
            f"""INSERT OR IGNORE INTO
            TimePeriod(period, flag)
            VALUES({vint}, 'e')"""
        )


    """
    ##############################################################
        References
    ##############################################################
    """

    # Add all references in the bibliography to the references tables
    for reference in config.refs:
        curs.execute(
            f"""REPLACE INTO
            DataSource(source_id, source, data_id)
            VALUES('{reference.id}', '{reference.citation}', "{utils.data_id()}")"""
        )

    
    """
    ##############################################################
        Data IDs
    ##############################################################
    """

    for id in sorted(config.data_ids):
        curs.execute(
            f"""REPLACE INTO
            DataSet(data_id)
            VALUES('{id}')"""
        )
    
    # Check for missing data IDs
    tables = [t[0] for t in curs.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]

    for table in tables:
        cols = [c[1] for c in curs.execute(f"PRAGMA table_info({table})").fetchall()]
        if "data_id" in cols:
            bad_rows = pd.read_sql_query(f"SELECT * FROM {table} WHERE data_id is NULL", conn)
            if len(bad_rows) > 0:
                print(f"Found some rows missing data IDs in {table}")
                print(bad_rows)

    
    conn.commit()
    conn.close()


    print(f"Post-aggregation complete.\n")



def aggregate_emissions():

    # Connect to the new database file
    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db
    

    """
    ##############################################################
        Emission Activity
    ##############################################################
    """

    emis_comm = config.params['emission_commodity']
    emis_units = config.params['emission_activity_units']

    # Get emissions factors for fuels in ktCO2eq/PJ_in
    emis_fact = utils.get_data('https://www.epa.gov/system/files/documents/2024-02/ghg-emission-factors-hub-2024.xlsx', skiprows=14, nrows=76, index_col=2)
    emis_fact = emis_fact[['CO2 Factor', 'CH4 Factor', 'N2O Factor']].iloc[1::].dropna()
    emis_fact = emis_fact[pd.to_numeric(emis_fact['CO2 Factor'], errors='coerce').notnull()] # Removing NaN rows
    for fact in emis_fact.columns: emis_fact[fact] = emis_fact[fact].astype(float) * conversion_factors['epa_units'][fact.strip(' Factor')] * conversion_factors['gwp'][fact.strip(' Factor')]
    emis_fact[emis_comm] = emis_fact.sum(axis=1)

    ref = config.refs.add('epa', config.params['epa_reference'])

    for tech in config.all_techs:

        # Valid vintages and efficiencies from Efficiency table
        rows = curs.execute(f"SELECT region, input_comm, tech, vintage, output_comm, efficiency FROM Efficiency WHERE tech == '{tech}'").fetchall()

        for row in rows:

            # Input fuel by epa naming convention
            epa_fuel = fuel_commodities.loc[fuel_commodities['comm'] == row[1], 'epa_fuel'].iloc[0]
            if pd.isna(epa_fuel): continue # doesn't need emissions

            # EmissionActivity is tied to OUTPUT energy so divide by efficiency
            emis_act = emis_fact.loc[epa_fuel, emis_comm] / row[5]

            # Note assumed fuel
            note = f"Emissions factor using {epa_fuel} (EPA, {config.params['epa_year']}) divided by efficiency as emissions are per output unit energy."

            curs.execute(
                f"""REPLACE INTO
                EmissionActivity(region, emis_comm, input_comm, tech, vintage, output_comm, activity, units,
                notes, data_source, dq_cred, dq_geog, dq_struc, dq_tech, dq_time, data_id)
                VALUES('{row[0]}', '{emis_comm}', '{row[1]}', '{row[2]}', {row[3]}, '{row[4]}', {emis_act}, '{emis_units}',
                '{note}', '{ref.id}', 1, 2, 3, 3, 2, '{utils.data_id(row[0])}')"""
            )
    

    conn.commit()
    conn.close()

    print(f"Emissions data aggregated into {os.path.basename(config.database_file)}\n")



def aggregate_imports():

    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor()

    # Get which fuel commodities are actually being used
    df_eff = pd.read_sql_query("SELECT * FROM Efficiency", conn)

    for tech, row in config.import_techs.iterrows():
        
        # Get CANOE nomenclature for imported commodity
        out_comm = config.fuel_commodities.loc[row['out_comm']]

        # Make sure the model is using this imported commodity otherwise skip
        if out_comm['comm'] not in df_eff['input_comm'].values:
            print(out_comm['comm'])
            continue
        
        description = f"import dummy for {out_comm['description']}"

        curs.execute(
            f"""REPLACE INTO
            Technology(tech, flag, sector, description)
            VALUES('{tech}', 'p', 'commercial', '{description}')"""
        )
        
        # A single vintage at first model period with no other parameters, classic dummy tech
        for region in config.model_regions:

            # Make sure the model is using this imported commodity in this region otherwise skip
            if df_eff[(df_eff['regions'] == region) & (df_eff['input_comm'] == out_comm['comm'])].empty:
                print(f"Import {tech} skipped for region {region} as the fuel isn't used.")
                continue

            curs.execute(
                f"""REPLACE INTO
                Efficiency(region, input_comm, tech, vintage, output_comm, efficiency, notes)
                VALUES('{region}', '{config.fuel_commodities.loc[row['in_comm'], 'comm']}', '{tech}',
                '{config.model_periods[0]}', '{out_comm['comm']}', 1, '{description})')"""
            )
            
    conn.commit()
    conn.close()

    print(f"Imports aggregated into {os.path.basename(config.database_file)}\n")



if __name__ == "__main__":
    
    aggregate()