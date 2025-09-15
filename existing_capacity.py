"""
Aggregates existing stock space heating and cooling data and demand

To fully define this sub-sector, we must fully define existing stock with the equation:

    DEM = SEC x EFF = CAP x C2A x ACF

where:

    DEM = annual output energy, space heating (PJ out)
    SEC = annual secondary energy consumed (PJ in)
    EFF = efficiency of existing stock (PJ out / PJ in)
    CAP = existing capacity (PJ/y)
    C2A = capacity-to-activity ratio, the annual output energy if 100% utilised (PJ / PJ/y.y)
    ACF = annual capacity factor, the actual annual utilisation (PJ/y actual / PJ/y max)

Procedure:

    1. Annual secondary energy consumption (SEC) by end-use / fuel from NRCan comprehensive energy use database
    2. Assume technology shares using analogous region market shares from AEO commercial demand module
    3. Calculate average efficiency (EFF) of natural gas / electricity space heating from (3.)
    4. Use (2. and 4.) to calculate annual end-use service demand (DEM) as DEM = SEC x EFF
    5. Calculate normalised hourly demand profile (DSD) by summing all hourly demands from comstock and normalising
    6. Assume peak demand is full utilisation of existing stock so that ACF = mean(DSD) / max(DSD)
    7. C2A is just 1 if we match capacity and activity units (PJ vs. PJ/y)
    8. Calculate existing capacity as CAP = DEM / (ACF x C2A)

Written by Ian David Elder for the CANOE model
"""

from setup import config
import sqlite3
import utils
import pandas as pd
from currency_conversion import conv_curr

base_year = config.params['base_year']
aeo_ref = config.params['aeo_reference']
aeo_year = config.params['aeo_installed_year']
nrcan_ref = config.params['nrcan_reference']
comstock_year = config.params['comstock']['data_year']
comstock_ref = config.params['comstock']['reference']


def aggregate_region(region: str, df_dsd: pd.DataFrame) -> pd.DataFrame:

    df_exs = aggregate_existing_sphc(region, df_dsd)
    aggregate_other(region, df_exs, df_dsd)

    return df_exs



def aggregate_existing_sphc(region: str, df_dsd: pd.DataFrame) -> pd.DataFrame:

    """
    ##############################################################
        Calculate data for existing technologies
    ##############################################################
    """

    ## 1. Get secondary energy consumption by end use and fuel from NRCan Comprehensive Energy Use Database
    # Table 24: Space Heating Secondary Energy Use and GHG Emissions by Energy Source
    sec_sph = utils.get_compr_db(region, 24, 3, 8)[base_year].astype(float)


    # Aggregate heavy/light oil and propane/natural gas as we dont have that technological resolution
    sec_sph['oil'] = sec_sph['light fuel oil and kerosene'] + sec_sph['heavy fuel oil']
    sec_sph['natural gas'] = sec_sph['natural gas'] + sec_sph['other']
    #sec_sph['district'] = sec_sph['steam'] # TODO dont have technoeconomic data for district schemes yet so ignore
    sec_sph.drop(['other','steam','light fuel oil and kerosene','heavy fuel oil'], inplace=True)

    # Slice up using Statcan data if its an atlantic province
    sec_sph = get_atlantic_fractions(region, sec_sph)
        
    df_sph = pd.DataFrame(data=sec_sph.values, columns=['sec'])
    df_sph['end_use'] = 'space heating'
    df_sph['fuel'] = sec_sph.index

    # Cut off threshold
    sec_tot = df_sph['sec'].sum()
    df_sph = df_sph.loc[(df_sph['sec'] / sec_tot) > config.params['existing_capacity_tolerance']]

    # Table 32: Space Cooling Secondary Energy Use and GHG Emissions by Energy Source
    sec_spc = utils.get_compr_db(region, 32, 3, 5)[base_year].astype(float)

    # Slice up using Statcan data if its an atlantic province
    sec_spc = get_atlantic_fractions(region, sec_spc)

    df_spc = pd.DataFrame(data=sec_spc.values, columns=['sec'])
    df_spc['end_use'] = 'space cooling'
    df_spc['fuel'] = sec_spc.index
    
    # Cut off threshold
    sec_tot = df_spc['sec'].sum()
    df_spc = df_spc.loc[(df_spc['sec'] / sec_tot) > config.params['existing_capacity_tolerance']]

    # Getting things into a single dataframe for tidier handling
    df_exs = pd.concat([df_sph, df_spc])
    df_exs.set_index(['end_use','fuel'], inplace=True)


    ## 2. Estimate existing stock efficiencies by end use and fuel from installed market shares in AEO CDM
    cdm_exs = config.aeo_cdm.copy() # for aggregated technoeconomic data of existing capacity

    # Get installed base for space heating and cooling for this region
    cdm_exs = cdm_exs.loc[((cdm_exs['serv'] == 'space heating') | (cdm_exs['serv'] == 'space cooling'))] # space heating or cooling
    cdm_exs = cdm_exs.loc[cdm_exs['reg'] == config.regions.loc[region, 'us_census_div']] # in region
    cdm_exs = cdm_exs.loc[~cdm_exs['techname'].str.contains('chiller')] # exclude chillers because we are only interested in heat pump-relevant space cooling
    cdm_exs.rename({'share': 'serv_share'}, inplace=True, axis='columns')
    cdm_exs = cdm_exs.loc[cdm_exs['serv_share'] > 0] # service energy share > 0 is installed base

    # Convert service energy share to secondary energy consumption share by dividing by efficiencies
    # Renormalise service and secondary energy shares for each end use and fuel, to get representative values for existing stock
    cdm_exs['sec_share'] = cdm_exs['serv_share']
    for end_use in cdm_exs['serv'].unique():
        for fuel in cdm_exs['fuel'].unique():
        
            df = cdm_exs.loc[(cdm_exs['serv'] == end_use) & (cdm_exs['fuel'] == fuel)].copy()
            df['sec_share'] = df['serv_share'] / df['efficiency']
            df['sec_share'] = df['sec_share'] / df['sec_share'].sum()
            df['serv_share'] = df['serv_share'] / df['serv_share'].sum()
            cdm_exs.loc[(cdm_exs['serv'] == end_use) & (cdm_exs['fuel'] == fuel), 'sec_share'] = df['sec_share']
            cdm_exs.loc[(cdm_exs['serv'] == end_use) & (cdm_exs['fuel'] == fuel), 'serv_share'] = df['serv_share']


    ## 3. Get average efficiency (and life) for each end use and fuel
    cdm_exs['avg_eff'] = cdm_exs['efficiency'] * cdm_exs['sec_share'] # eff indexed to secondary energy share
    cdm_exs['avg_life'] = (cdm_exs['life'] * cdm_exs['serv_share']).round() # life indexed to output service energy share
    cdm_exs['avg_fixed_cost'] = cdm_exs['maintcst'] * cdm_exs['serv_share'] # fixed cost indexed to output service energy share
    
    cdm_exs = cdm_exs.groupby(['serv','fuel']).sum()
    df_exs = df_exs.drop([euf for euf in df_exs.index if euf not in cdm_exs.index]) # no service share so drop this end-use-fuel combo
    for col in ['avg_eff','avg_life','avg_fixed_cost']:
        df_exs[col] = df_exs.index.map(lambda euf: cdm_exs.loc[euf, col])


    ## 4. Multiply secondary energies from NRCan by average efficiencies for each end use and fuel to get demanded output energies
    df_exs['dem'] = df_exs.index.map(lambda euf: df_exs.loc[euf, 'sec'] * cdm_exs.loc[euf, 'avg_eff'])
    df_dem = df_exs['dem'].groupby('end_use').sum()

    
    ## 5. Calculate normalised hourly demand profile (DSD) by summing all hourly demands from comstock and normalising
    # Already done outside function


    ## 6. Assume peak demand is full utilisation of existing stock so that ACF = mean(DSD) / max(DSD)
    df_acf = pd.DataFrame(index=df_dsd.columns, columns=['acf'], data=[df_dsd[col].mean() / df_dsd[col].max() for col in df_dsd.columns])
    df_exs['acf'] = df_exs.index.map(lambda euf: df_acf.loc[f"{euf[0]} {euf[1]}", 'acf'])


    ## 7. C2A is just 1 if we match capacity and activity units (PJ vs. PJ/y)
    df_exs['c2a'] = 1


    ## 8. Calculate existing capacity as CAP = DEM / (ACF x C2A) = DEM / ACF
    df_exs['cap'] = df_exs['dem'] / df_exs['acf']

    
    ## Save calculated existing data to local cache for review
    df_exs.to_csv(config.cache_dir + f"calculated_existing_sphc_data_{region.lower()}.csv")
    print(f"Saved calculated {region} existing space heating and cooling data locally.")



    """
    ##############################################################
        Prepare some common data
    ##############################################################
    """

    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor()

    exs_techs = config.existing_techs


    """
    ##############################################################
        Demands
    ##############################################################
    """

    config.refs.add('comstock', comstock_ref)
    config.refs.add('demand', f"{nrcan_ref}; {aeo_ref}; {config.params['gdp_reference']}")
    
    for end_use, dem in df_dem.items():

        if dem == 0: continue

        eu_config = config.end_use_demands.loc[end_use]
        dsd = df_dsd[end_use].to_numpy()
        
        ann_dem = dem * config.gdp_index # annual demand indexed to gdp growth

        ## Commodities
        curs.execute(
            f"""REPLACE INTO
            Commodity(name, flag, description, data_id)
            VALUES('{eu_config['comm']}', 'd', '({eu_config['dem_unit']}) {eu_config['description']}', '{utils.data_id()}')"""
        )


        ## DemandSpecificDistribution
        if config.params['include_dsd']:
            data = []
            ref = config.refs.get('comstock')

            print(f"Adding DSD for {end_use} demand in {region}...")

            for period in config.model_periods:
                for h, row in config.time.iterrows():

                    # Data descriptors take up a good bit of storage for timeseries data so only attach to first hour of each day
                    if h % 24 == 0:
                        data.append([
                            region, period, row['season'], row['tod'], eu_config['comm'], dsd[h],
                            "Comstock hourly consumption for lighting and equipment summed over all building types and normalised",
                            ref.id,
                            1, 2, 1, 2, 3,
                            utils.data_id(region),
                        ])
                    else:
                        data.append([
                            region, period, row['season'], row['tod'], eu_config['comm'], dsd[h],
                            None, None, None, None, None, None, None,
                            utils.data_id(region),
                        ])
                
            
            conn.executemany(
                f"""REPLACE INTO
                DemandSpecificDistribution(region, period, season, tod, demand_name, dsd,
                notes, data_source, dq_cred, dq_geog, dq_struc, dq_tech, dq_time, data_id) 
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                data
            )


        ## Demand
        ref = config.refs.get('demand')

        for period in config.model_periods:
            
            dem = ann_dem.loc[period].iloc[0]
            note = (f"Efficiency (AEO, {aeo_year}) times secondary energy consumption (NRCan, {base_year}) "
                    f"indexed to projected provincial gdp growth (CER, {config.params['gdp_data_year']})")

            curs.execute(
                f"""REPLACE INTO
                Demand(region, period, commodity, demand, units,
                notes, data_source, dq_cred, dq_geog, dq_struc, dq_tech, dq_time, data_id) 
                VALUES('{region}', {period}, '{eu_config['comm']}', {dem}, '({eu_config['dem_unit']})',
                '{note}', '{ref.id}', 1, 2, 2, 2, 3, '{utils.data_id(region)}')"""
            )
            


    """
    ##############################################################
        Existing technologies
    ##############################################################
    """

    config.refs.add('nrcan_aeo', f"{nrcan_ref}; {aeo_ref}")
    config.refs.add('nrcan_aeo_comstock', f"{nrcan_ref}; {aeo_ref}; {comstock_ref}")

    for tech, tech_config in exs_techs.iterrows():

        if tech_config['end_use'] != 'space heating' and tech_config['end_use'] != 'space cooling': continue
        if (tech_config['end_use'], tech_config['fuel']) not in df_exs.index: continue # no existing data for this technology

        fuel_config = config.fuel_commodities.loc[tech_config['fuel']]
        eu_config = config.end_use_demands.loc[tech_config['end_use']]
        exs_data = df_exs.loc[(tech_config['end_use'], tech_config['fuel'])]

        if exs_data['cap'] == 0: continue # no existing capacity -> no process

        # NOT CURRENTLY IN USE - omitting minACF instead
        # Annual capacity factor cannot be higher than the area under the DSD curve
        # otherwise the peak output of a process must be higher than its capacity allows (impossible)
        # since all end use outputs follow the same normalised curve as the DSD
        #dsd = df_dsd[end_use]
        #acf_lim = dsd.mean() / dsd.max() * (1 - config.params['acf_buffer'])


        ## Technologies
        curs.execute(
            f"""REPLACE INTO
            Technology(tech, flag, sector, annual, description, data_id)
            VALUES('{tech}', 'p', 'commercial', 1, '{tech_config['end_use']} {tech_config['description']}', '{utils.data_id()}')"""
        )


        ## LifetimeTech
        life = round(cdm_exs.loc[(tech_config['end_use'], tech_config['fuel']), 'avg_life'])
        note = f"Average life of installed stock indexed to shares of service demand by end use and fuel (AEO, {aeo_year})"
        ref = config.refs.get('aeo')
        curs.execute(
            f"""REPLACE INTO
            LifetimeTech(region, tech, lifetime,
            notes, data_source, dq_cred, dq_geog, dq_struc, dq_tech, dq_time, data_id) 
            VALUES('{region}', '{tech}', {life},
            '{note}', '{ref.id}', 1, 2, 1, 2, 3, '{utils.data_id(region)}')"""
        )
        

        ## CapacityToActivity
        c2a = 1 # Capacity is in PJ/y and activity is in PJ
        note = "Capacity is in PJ/y and activity is in PJ so 1"
        curs.execute(
            f"""REPLACE INTO
            CapacityToActivity(region, tech, c2a, notes, data_id)
            VALUES('{region}', '{tech}', {c2a}, '{note}', '{utils.data_id(region)}')"""
        )
        

        # Spread existing capacity evenly over existing vintages
        vints, weights = utils.stock_vintages(base_year, life)

        # Only indexed by vintage
        for v in range(len(vints)):

            vint = vints[v]
            weight = weights[v]

            if vint + life <= config.model_periods[0]: continue

            ## Efficiency
            eff = exs_data['avg_eff']
            note = ("Average efficiency of installed stock estimated using shares of secondary energy consumption. "
                    f"Secondary energy consumption shares calculated from fuel share by end use (NRCan, {base_year}) "
                    f"further indexed to service demand shares divided by efficiencies for installed base technologies "
                    f"of the same end use and fuel (AEO, {aeo_year}).")
            ref = config.refs.get('nrcan_aeo')
            curs.execute(
                f"""REPLACE INTO
                Efficiency(region, input_comm, tech, vintage, output_comm, efficiency,
                notes, data_source, dq_cred, dq_geog, dq_struc, dq_tech, dq_time, data_id)
                VALUES('{region}', '{fuel_config['comm']}', '{tech}', {vint}, '{eu_config['comm']}', {eff},
                '{note}', '{ref.id}', 1, 2, 3, 2, 2, '{utils.data_id(region)}')"""
            )
            

            ## ExistingCapacity
            cap = weight * exs_data['cap']
            note = (f"Secondary energy consumption shares calculated from fuel share by end use (NRCan, {base_year}) "
                    f"times average efficiency for installed base technologies (AEO, {aeo_year}) "
                    f"divided by estimated annual capacity factor (NREL, {comstock_year})")
            ref = config.refs.get('nrcan_aeo_comstock')
            curs.execute(
                f"""REPLACE INTO
                ExistingCapacity(region, tech, vintage, capacity, units,
                notes, data_source, dq_cred, dq_geog, dq_struc, dq_tech, dq_time, data_id)
                VALUES('{region}', '{tech}', {vint}, {cap}, '({eu_config['cap_unit']})',
                '{note}', '{ref.id}', 1, 2, 2, 2, 1, '{utils.data_id(region)}')"""
            )
            

            # Indexed by period and vintage
            for period in config.model_periods:

                if vint > period or vint + life <= period: continue

                ## CostFixed
                cost_fixed = exs_data['avg_fixed_cost'] * config.params['conversion_factors']['cost']['aeo']
                cost_fixed = conv_curr(cost_fixed)
                note = f"Average maintenance cost of installed stock indexed to shares of service demand by end use and fuel (AEO, {aeo_year})"
                ref = config.refs.get('aeo')
                curs.execute(
                    f"""REPLACE INTO
                    CostFixed(region, period, tech, vintage, cost, units,
                    notes, data_source, dq_cred, dq_geog, dq_struc, dq_tech, dq_time, data_id)
                    VALUES('{region}', {period}, '{tech}', {vint}, {cost_fixed}, 'M$/PJ',
                    '{note}', '{ref.id}', 1, 2, 1, 2, 2, '{utils.data_id(region)}')"""
                )


        ## AnnualCapacityFactor
        acf = exs_data['acf']
        note = f"Mean hourly demand divided by peak hourly demand from Comstock (NREL, {comstock_year})"
        ref = config.refs.get('comstock')

        for period in config.model_periods:

            if max(vints) + life <= period: continue # no vintage would live this long

            curs.execute(
                f"""REPLACE INTO
                LimitAnnualCapacityFactor(region, period, tech, output_comm, operator, factor,
                notes, data_source, dq_cred, dq_geog, dq_struc, dq_tech, dq_time, data_id)
                VALUES('{region}', {period}, '{tech}', '{eu_config['comm']}', 'le', {acf},
                '{note}', '{ref.id}', 1, 2, 5, 2, 3, '{utils.data_id(region)}')"""
            )
            

    conn.commit()
    conn.close()

    return df_exs



def aggregate_other(region: str, df_exs: pd.DataFrame, df_dsd: pd.DataFrame):

    eu_config: pd.Series = config.end_use_demands.loc['other']
    tech_config: pd.Series = config.new_techs.loc[config.new_techs['end_use'] == 'other'].iloc[0]
    dsd = df_dsd['other'].to_numpy() # faster
    elc_fact = config.params['other_electrification_factor']

    if not tech_config['include_new']: return # maybe someone will want to skip all this

    tech = tech_config.name
    vint = config.model_periods[0] # dummy tech

    """
    ##############################################################
        All demands other than space heating and cooling
    ##############################################################
    """

    # Secondary energy by fuel from space heating and cooling (already accounted for)
    sec_sphc = df_exs.groupby(['fuel']).sum()['sec']

    # Table 1: Secondary Energy Use and GHG Emissions by Energy Source
    sec: pd.Series = utils.get_compr_db(region, 1, 3, 8)[base_year].astype(float)

    # Slice up using Statcan data if its an atlantic province
    sec = get_atlantic_fractions(region, sec)

    # Demand is sum of secondary energies minus those from space heating and cooling (already accounted for)
    ann_dem = (sec.sum() - sec_sphc.sum()) * config.gdp_index # annual demand indexed to gdp growth

    # Aggregate heavy/light oil and propane/natural gas as we dont have that technological resolution
    sec['oil'] = sec['light fuel oil and kerosene'] + sec['heavy fuel oil']
    sec['natural gas'] = sec['natural gas'] + sec['other']
    #sec_sph['district'] = sec_sph['steam'] # TODO dont have technoeconomic data for district schemes yet so ignore
    sec.drop(['other','steam','light fuel oil and kerosene','heavy fuel oil'], inplace=True)

    # TechInputSplit ratios are fuel shares of residual secondary energy consumption
    for fuel in sec.index.difference(sec_sphc.index): sec_sphc[fuel] = 0
    sec_oth = sec - sec_sphc
    ti_splits = sec_oth / sec_oth.sum()


    """
    ##############################################################
        Add to database
    ##############################################################
    """
    
    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor()


    ## Technologies
    curs.execute(
        f"""REPLACE INTO
        Technology(tech, flag, sector, annual, unlim_cap, description, data_id)
        VALUES('{tech}', 'p', 'commercial', 1, 1, '{tech_config['end_use']} {tech_config['description']}', '{utils.data_id()}')"""
    )

    ## Commodities
    curs.execute(
        f"""REPLACE INTO
        Commodity(name, flag, description, data_id)
        VALUES('{eu_config['comm']}', 'd', '({eu_config['dem_unit']}) {eu_config['description']}', '{utils.data_id()}')"""
    )


    config.refs.add('nrcan_cef', f"{nrcan_ref}; {config.params['cef_reference']}")

    # Flows
    for fuel in sec_oth.index:

        fuel_config = config.fuel_commodities.loc[fuel]

        ## Efficiency
        note = "Dummy tech. Demand equal to secondary energy consumption"
        curs.execute(
            f"""REPLACE INTO
            Efficiency(region, input_comm, tech, vintage, output_comm, efficiency, notes, data_id)
            VALUES('{region}', '{fuel_config['comm']}', '{tech}', {vint}, '{eu_config['comm']}', 1, '{note}', '{utils.data_id(region)}')"""
        )
        

        ## TechInputSplit
        for period in config.model_periods:

            tis = ti_splits[fuel]

            # Linear interpolation towards reducing non-elc fuels by electrification factor
            lin_f = (period - base_year)/(config.model_periods[-1] - base_year) # 0 -> 1 linear factor over time
            if fuel == 'electricity': target = elc_fact + tis * ( 1 - elc_fact ) # elc increases
            else: target = tis * (1 - elc_fact) # all others decrease by elc_fact

            tis = tis + (target - tis) * lin_f # elc -> 1

            note = f"Secondary energy consumption by fuel (NRCan, {base_year}) minus space heating and cooling. {config.params['cef_note']}"
            ref = config.refs.get('nrcan_cef')
            curs.execute(
                f"""REPLACE INTO
                LimitTechInputSplit(region, period, input_comm, tech, operator, proportion,
                notes, data_source, dq_cred, dq_geog, dq_struc, dq_tech, dq_time, data_id)
                VALUES('{region}', {period}, '{fuel_config['comm']}', '{tech}', 'le', {tis},
                '{note}', '{ref.id}', 1, 1, 5, 1, 1, '{utils.data_id(region)}')"""
            )


    ## DemandSpecificDistribution
    if config.params['include_dsd']:
        data = []
        ref = config.refs.get('comstock')

        print(f"Adding DSD for other demand in {region}...")

        for period in config.model_periods:
            for h, row in config.time.iterrows():

                # Data descriptors take up a good bit of storage for timeseries data so only attach to first hour of each day
                if h % 24 == 0:
                    data.append([
                        region, period, row['season'], row['tod'], eu_config['comm'], dsd[h],
                        "Comstock hourly consumption for lighting and equipment summed over all building types and normalised",
                        ref.id,
                        1, 2, 1, 2, 3,
                        utils.data_id(region),
                    ])
                else:
                    data.append([
                        region, period, row['season'], row['tod'], eu_config['comm'], dsd[h],
                        None, None, None, None, None, None, None,
                        utils.data_id(region),
                    ])
            
        
        conn.executemany(
            f"""REPLACE INTO
            DemandSpecificDistribution(region, period, season, tod, demand_name, dsd,
            notes, data_source, dq_cred, dq_geog, dq_struc, dq_tech, dq_time, data_id) 
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            data
        )


    ## Demand
    ref = config.refs.add('nrcan_gdp', f"{nrcan_ref}; {config.params['gdp_reference']}")
    for period in config.model_periods:
        
        dem = ann_dem.loc[period].iloc[0]
        
        dem = ann_dem.loc[period].iloc[0]
        note = f"Annual secondary energy consumption summed over all fuels minus space heating and cooling (NRCan, {base_year})"

        curs.execute(
            f"""REPLACE INTO
            Demand(region, period, commodity, demand, units,
            notes, data_source, dq_cred, dq_geog, dq_struc, dq_tech, dq_time, data_id) 
            VALUES('{region}', {period}, '{eu_config['comm']}', {dem}, '({eu_config['dem_unit']})',
            '{note}', '{ref.id}', 1, 2, 2, 2, 3, '{utils.data_id(region)}')"""
        )

    conn.commit()
    conn.close()



def get_atlantic_fractions(region: str, sec: pd.Series) -> pd.Series:
    """
    For the comprehensive energy use database in commercial, the atlantic provinces are all aggregated.
    To slice them up, we use energy proportions from Statcan data. The system scope of the Statcan
    data is different from NRCan, including upstream energy use, so we dont want to use it directly.
    """

    if not config.regions.loc[region]['atlantic']: return sec

    # Get the primary and secondary energy use table
    df = utils.get_statcan_table(
        25100029,
        'statcan_atlantic_energy',
        usecols = ['REF_DATE','GEO','Fuel type','Supply and demand characteristics','VALUE'],
        filter = lambda df: df.loc[
            (df['REF_DATE'] == config.params['base_year'])
            & (df['Fuel type'].isin(config.fuel_commodities['statcan_fuel']))
            & (df['Supply and demand characteristics'] == 'Commercial and other institutional')
            & (df['GEO'].str.lower().isin(config.regions['description'].loc[config.regions['atlantic']]))
        ]
    ).fillna(0)

    # Map over regions and fuels
    df['region'] = df['GEO'].str.lower().map({
        config.regions.loc[idx, 'description']: idx for idx in config.regions.index
    })
    df['fuel'] = df['Fuel type'].map({
        config.fuel_commodities.loc[idx, 'statcan_fuel']: idx for idx in config.fuel_commodities.index
    })

    # Get the total atlantic energy consumption by fuel
    df_fuel = df.groupby('fuel')['VALUE'].sum()

    # Get fractions of each atlantic province of the total by fuel
    for idx, row in df.iterrows():
        df.loc[idx, 'fraction'] = row['VALUE'] / df_fuel.loc[row['fuel']]

    # Return these fractions
    df = df.loc[df['region'] == region]
    df = df.set_index('fuel')['fraction']

    # Dice up secondary energy using Statcan proportions
    sec = sec.copy()
    for fuel in sec.index:
        if fuel not in df.index:
            sec[fuel] = 0
        else:
            sec[fuel] *= df.loc[fuel]

    return sec



if __name__ == "__main__":

    for region in config.model_regions: aggregate_region(region)