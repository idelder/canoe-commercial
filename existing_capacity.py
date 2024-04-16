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


    df_sph = pd.DataFrame(data=sec_sph.values, columns=['sec'])
    df_sph['end_use'] = 'space heating'
    df_sph['fuel'] = sec_sph.index

    # Cut off threshold
    sec_tot = df_sph['sec'].sum()
    df_sph = df_sph.loc[(df_sph['sec'] / sec_tot) > config.params['existing_capacity_tolerance']]

    # Table 32: Space Cooling Secondary Energy Use and GHG Emissions by Energy Source
    sec_spc = utils.get_compr_db(region, 32, 3, 5)[base_year].astype(float)

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
    print(f"Cached calculated {region} existing space heating and cooling data locally.")



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

    for end_use, dem in df_dem.items():

        if dem == 0: continue

        eu_config = config.end_use_demands.loc[end_use]
        dsd = df_dsd[end_use].to_numpy()

        ann_dem = dem * config.gdp_index[region] # annual demand indexed to gdp growth

        ## Commodities
        curs.execute(f"""REPLACE INTO
                     commodities(comm_name, flag, comm_desc)
                     VALUES('{eu_config['comm']}', 'd', '({eu_config['dem_unit']}) {eu_config['description']}')""")


        ## DemandSpecificDistribution
        if config.params['include_dsd']:
            for h, t_config in config.time.iterrows():

                # Data descriptors take up a good bit of storage for timeseries data so only attach to first hour of each day
                if h % 24 == 0:
                    note = f"Comstock hourly consumption summed over all building types and normalised (NREL, {comstock_year})"
                    reference = comstock_ref
                else: note=reference=''
                
                curs.execute(f"""REPLACE INTO
                            DemandSpecificDistribution(regions, season_name, time_of_day_name, demand_name, dsd, dsd_notes,
                            reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                            VALUES('{region}', '{t_config['season']}', '{t_config['time_of_day']}', '{eu_config['comm']}', {dsd[h]}, '{note}',
                            '{reference}', {comstock_year}, 2, 1, 1, 1, 3, 3)""")


        ## Demand
        for period in config.model_periods:
            
            dem = ann_dem.loc[period]
            note = (f"Efficiency (AEO, {aeo_year}) times secondary energy consumption (NRCan, {base_year}) "
                    f"indexed to projected provincial gdp growth (CER, {config.params['gdp_data_year']})")
            reference = f"{nrcan_ref}; {aeo_ref}; {config.params['gdp_reference']}"
            curs.execute(f"""REPLACE INTO
                        Demand(regions, periods, demand_comm, demand, demand_units, demand_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', {period}, '{eu_config['comm']}', {dem}, '({eu_config['dem_unit']})', '{note}',
                        '{reference}', {base_year}, 3, 1, 1, 1, 3, 3)""")


    """
    ##############################################################
        Existing technologies
    ##############################################################
    """

    for tech, tech_config in exs_techs.iterrows():

        if tech_config['end_use'] != 'space heating' and tech_config['end_use'] != 'space cooling': continue
        if (tech_config['end_use'], tech_config['fuel']) not in df_exs.index: continue # no existing data for this technology

        fuel_config = config.fuel_commodities.loc[tech_config['fuel']]
        eu_config = config.end_use_demands.loc[tech_config['end_use']]
        exs_data = df_exs.loc[(tech_config['end_use'], tech_config['fuel'])]

        if exs_data['cap'] == 0: continue # no existing capacity -> no process

        # Annual capacity factor cannot be higher than the area under the DSD curve
        # otherwise the peak output of a process must be higher than its capacity allows (impossible)
        # since all end use outputs follow the same normalised curve as the DSD
        dsd = df_dsd[end_use]
        acf_lim = dsd.mean() / dsd.max() * (1 - config.params['acf_buffer'])


        ## Technologies
        curs.execute(f"""REPLACE INTO
                     technologies(tech, flag, sector, tech_desc)
                     VALUES('{tech}', 'p', 'commercial', '{tech_config['end_use']} {tech_config['description']}')""")


        ## LifetimeTech
        life = round(cdm_exs.loc[(tech_config['end_use'], tech_config['fuel']), 'avg_life'])
        note = f"Average life of installed stock indexed to shares of service demand by end use and fuel (AEO, {aeo_year})"
        reference = aeo_ref
        curs.execute(f"""REPLACE INTO
                    LifetimeTech(regions, tech, life, life_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', '{tech}', {life}, '{note}',
                    '{reference}', {aeo_year}, 2, 1, 1, 1, 3, 3)""")
        

        ## CapacityToActivity
        c2a = 1 # Capacity is in PJ/y and activity is in PJ
        note = "Capacity is in PJ/y and activity is in PJ so 1"
        curs.execute(f"""REPLACE INTO
                    CapacityToActivity(regions, tech, c2a, c2a_notes)
                    VALUES('{region}', '{tech}', {c2a}, '{note}')""")
        

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
            reference = f"{nrcan_ref}; {aeo_ref}"
            curs.execute(f"""REPLACE INTO
                        Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{fuel_config['comm']}', '{tech}', {vint}, '{eu_config['comm']}', {eff}, '{note}',
                        '{reference}', {aeo_year}, 2, 2, 1, 1, 3, 3)""")
            

            ## ExistingCapacity
            cap = weight * exs_data['cap']
            note = (f"Secondary energy consumption shares calculated from fuel share by end use (NRCan, {base_year}) "
                    f"times average efficiency for installed base technologies (AEO, {aeo_year}) "
                    f"divided by estimated annual capacity factor (NREL, {comstock_year})")
            reference = f"{nrcan_ref}; {aeo_ref}; {comstock_ref}"
            curs.execute(f"""REPLACE INTO
                        ExistingCapacity(regions, tech, vintage, exist_cap, exist_cap_units, exist_cap_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{tech}', {vint}, {cap}, '({eu_config['cap_unit']})', '{note}',
                        '{reference}', {base_year}, 4, 2, 1, {utils.dq_time(comstock_year, period)}, 3, 3)""")
            

            # Indexed by period and vintage
            for period in config.model_periods:

                if vint > period or vint + life <= period: continue

                ## CostFixed
                cost_fixed = exs_data['avg_fixed_cost'] * config.params['conversion_factors']['cost']['aeo']
                note = f"Average maintenance cost of installed stock indexed to shares of service demand by end use and fuel (AEO, {aeo_year})"
                reference = aeo_ref
                curs.execute(f"""REPLACE INTO
                            CostFixed(regions, periods, tech, vintage, cost_fixed_notes, data_cost_fixed, data_cost_year, data_curr,
                            reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                            VALUES('{region}', {period}, '{tech}', {vint}, '{note}', {cost_fixed}, {config.params['aeo_currency_year']}, '{config.params['aeo_currency']}',
                            '{reference}', {aeo_year}, 2, 1, 1, 1, 3, 3)""")


        ## AnnualCapacityFactor
        acf = exs_data['acf']
        note = f"Mean hourly demand divided by peak hourly demand from Comstock (NREL, {comstock_year})"
        reference = comstock_ref

        if acf > acf_lim:
                acf = acf_lim
                note += f". Bounded to mean(DSD)/max(DSD) for {tech_config['end_use']}"
                print(f"Warning! Annual capacity factor for {region} {tech} {tech_config['end_use']} was too high and had to be bounded. "
                      "ACF cannot be higher than mean(DSD)/max(DSD) or the model will have no solution.")

        for period in config.model_periods:

            if max(vints) + life <= period: continue # no vintage would live this long

            curs.execute(f"""REPLACE INTO
                        MinAnnualCapacityFactor(regions, periods, tech, output_comm, min_acf, min_acf_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', {period}, '{tech}', '{eu_config['comm']}', {acf*0.99}, '{note}. Times 0.99 for computational slack.',
                        '{reference}', {comstock_year}, 4, 2, 1, {utils.dq_time(comstock_year, period)}, 3, 3)""")
            curs.execute(f"""REPLACE INTO
                        MaxAnnualCapacityFactor(regions, periods, tech, output_comm, max_acf, max_acf_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', {period}, '{tech}', '{eu_config['comm']}', {acf}, '{note}',
                        '{reference}', {comstock_year}, 4, 2, 1, {utils.dq_time(comstock_year, period)}, 3, 3)""")
            

    conn.commit()
    conn.close()

    return df_exs



def aggregate_other(region: str, df_exs: pd.DataFrame, df_dsd: pd.DataFrame):

    eu_config: pd.Series = config.end_use_demands.loc['other']
    tech_config: pd.Series = config.new_techs.loc[config.new_techs['end_use'] == 'other'].iloc[0]
    dsd = df_dsd['other'].to_numpy() # faster

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
    sec = utils.get_compr_db(region, 1, 3, 8)[base_year].astype(float)

    # Demand is sum of secondary energies minus those from space heating and cooling (already accounted for)
    ann_dem = (sec.sum() - sec_sphc.sum()) * config.gdp_index[region] # annual demand indexed to gdp growth

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
    curs.execute(f"""REPLACE INTO
                technologies(tech, flag, sector, tech_desc)
                VALUES('{tech}', 'p', 'commercial', '{tech_config['end_use']} {tech_config['description']}')""")

    ## Commodities
    curs.execute(f"""REPLACE INTO
                commodities(comm_name, flag, comm_desc)
                VALUES('{eu_config['comm']}', 'd', '({eu_config['dem_unit']}) {eu_config['description']}')""")


    # Flows
    for fuel in sec_oth.index:

        fuel_config = config.fuel_commodities.loc[fuel]

        ## Efficiency
        note = "Dummy tech. Demand equal to secondary energy consumption"
        curs.execute(f"""REPLACE INTO
                    Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes)
                    VALUES('{region}', '{fuel_config['comm']}', '{tech}', {vint}, '{eu_config['comm']}', 1, '{note}')""")
        

        ## TechInputSplit
        for period in config.model_periods:

            tis = ti_splits[fuel]

            # Linear interpolation towards only electricity
            lin_f = (period - base_year)/(config.model_periods[-1] - base_year)
            if fuel == 'electricity': tis = tis + (1 - tis) * lin_f # elc -> 1
            else: tis = tis * (1 - lin_f) # all others -> 0

            note = (f"Secondary energy consumption by fuel (NRCan, {base_year}) minus space heating and cooling. "
                    f"Linear interpolation from {base_year} shares to exclusively electricity. "
                    "Simple assumption to make a net-zero constraint feasible.")
            reference = nrcan_ref
            curs.execute(f"""REPLACE INTO
                        TechInputSplit(regions, periods, input_comm, tech, ti_split, ti_split_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', {period}, '{fuel_config['comm']}', '{tech}', {tis}, '{note}',
                        '{reference}', {base_year}, 2, 2, 1, 1, 1, 1)""")



    ## DemandSpecificDistribution
    if config.params['include_dsd']:
        for h, t_config in config.time.iterrows():

            # Data descriptors take up a good bit of storage for timeseries data so only attach to first hour of each day
            if h % 24 == 0:
                note = f"Comstock hourly consumption for lighting and equipment summed over all building types and normalised (NREL, {comstock_year})"
                reference = comstock_ref
            else: note=reference=''
            
            curs.execute(f"""REPLACE INTO
                        DemandSpecificDistribution(regions, season_name, time_of_day_name, demand_name, dsd, dsd_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{t_config['season']}', '{t_config['time_of_day']}', '{eu_config['comm']}', {dsd[h]}, '{note}',
                        '{reference}', {comstock_year}, 2, 1, 1, 1, 3, 3)""")


    ## Demand
    for period in config.model_periods:
        
        dem = ann_dem.loc[period]
        note = f"Annual secondary energy consumption summed over all fuels minus space heating and cooling (NRCan, {base_year})"
        reference = f"{nrcan_ref}; {config.params['gdp_reference']}"
        curs.execute(f"""REPLACE INTO
                    Demand(regions, periods, demand_comm, demand, demand_units, demand_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', {period}, '{eu_config['comm']}', {dem}, '({eu_config['dem_unit']})', '{note}',
                    '{reference}', {base_year}, 2, 2, 1, 1, 1, 1)""")

    conn.commit()
    conn.close()



if __name__ == "__main__":

    for region in config.model_regions: aggregate_region(region)