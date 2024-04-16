This script aggregates the modular residential sector model for CANOE. It pulls data primarily from the NRCan Comprehensive Energy Use Database and from Annual Energy Outlook technology assumptions.

Aggregation can be configured through files in input_files/ (see Configuration) to include/exclude technologies, provinces, types of data/model structures.

It will download a large number of files on the first run (as of now, !_!_!_! MB) but will cache these files locally and use the local cache in subsequent runs. Parameters can be set to force downloading to get latest data (see Configuration).



=======
 Usage
=======

1. Create the conda environment.
	a. Install miniconda
	b. Open a miniconda prompt
	c. Install the conda environment
		i. Set current directory to residential_sector/
			> cd C:/.../residential_sector/
		ii. Create the environment
			> conda env create
2. Run the aggregation
	a. Set current directory to parent of /residential_sector
		> cd C:/.../foo/  (where /residential_sector/ is contained)
	b. Activate the environment
		> conda activate canoe-backend
	c. Run the scripts
		> python residential_sector/



============
 Components
============


 Directories
=============
- data_cache/
	Where downloaded data is locally cached and pulled from on subsequent runs, unless force_download param is set to True
- documentation/
	TODO: eventual directory of documents describing the operation of this sector backend.
- input_files/
	- Configuration files
	- CANOE database schema
	- CANOE excel spreadsheet template
	- params.yml (see Configuration)

 Scripts
=========
- __main__.py
	For running from command line like >python electricity_sector/
- residential_sector.py
	Handles execution order of other scripts.
- currency_conversion.py
	Converts data currencies to a unified final currency.
- setup.py
	Builds the config object which contains aggregation configuration and some common data.
- utils.py
	Repository of frequently used utility scripts. Includes get_data() method which robustly fetches and locally caches data from online sources.
- testing_dummy.py
	A blank script for prototyping code snippets.
- others...
	Aggregation scripts for particular data types.
- model_reduction
	[To be removed]



===============
 Configuration TODO document this in a pdf or word document instead with tables
===============

Configuration is performed by editing the following files in residential_sector/input_files


 params.yml
============
Various other aggregation parameters. Some cannot be changed but others can. Generally free to play with booleans under "## Aggregation switches"