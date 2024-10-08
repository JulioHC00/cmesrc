.PHONY: clean lint requirements sync_data_to_s3 sync_data_from_s3 install_requirements

#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
BUCKET = [OPTIONAL] your-bucket-for-syncing-data (do not include 's3://')
PROFILE = default
PROJECT_NAME = cmesrc
PYTHON_INTERPRETER = python3

ifeq (,$(shell which conda))
HAS_CONDA=False
else
HAS_CONDA=True
endif

#################################################################################
# ADD PROJECT ROOT FOLDER TO PATH												#
#################################################################################

export PYTHONPATH := $(PROJECT_DIR):$(PYTHONPATH)

#################################################################################
# COMMANDS                                                                      #
#################################################################################

## Install Python Dependencies
requirements: test_environment
	$(PYTHON_INTERPRETER) -m pip install -U pip setuptools wheel
	$(PYTHON_INTERPRETER) -m pip install -r requirements.txt

## Delete all compiled Python files
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete

## Lint using flake8
lint:
	flake8 src

## Set up python interpreter environment
create_environment:
ifeq (True,$(HAS_CONDA))
		@echo ">>> Detected conda, creating conda environment."
ifeq (3,$(findstring 3,$(PYTHON_INTERPRETER)))
	conda create --name $(PROJECT_NAME) python=3
else
	conda create --name $(PROJECT_NAME) python=2.7
endif
		@echo ">>> New conda env created. Activate with:\nsource activate $(PROJECT_NAME)"
else
	$(PYTHON_INTERPRETER) -m pip install -q virtualenv virtualenvwrapper
	@echo ">>> Installing virtualenvwrapper if not already installed.\nMake sure the following lines are in shell startup file\n\
	export WORKON_HOME=$$HOME/.virtualenvs\nexport PROJECT_HOME=$$HOME/Devel\nsource /usr/local/bin/virtualenvwrapper.sh\n"
	@bash -c "source `which virtualenvwrapper.sh`;mkvirtualenv $(PROJECT_NAME) --python=$(PYTHON_INTERPRETER)"
	@echo ">>> New virtualenv created. Activate with:\nworkon $(PROJECT_NAME)"
endif

## Test python environment is setup correctly
test_environment:
	$(PYTHON_INTERPRETER) test_environment.py

#################################################################################
# PROJECT RULES                                                                 #
#################################################################################

ORIG_SWAN := $(wildcard ./data/raw/mvts/DT_SWAN/*.csv)
UPDATED_SWAN := $(patsubst ./data/raw/mvts/DT_SWAN/%.csv, ./data/interim/SWAN/%.csv, $(ORIG_SWAN))

.PHONY: generate_catalogue
generate_catalogue: ./data/processed/cmesrc.db

## Fill missing SWAN positions

$(UPDATED_SWAN): ./src/scripts/pre-processing/fill_swan_missing_positions.py $(ORIG_SWAN)
	@python3 $<

## Pre-load these
./data/processed/cmesrc_BBOXES.db: ./src/scripts/catalogue/pre_data_loading.py $(UPDATED_SWAN)
	@python3 $<

## Create HARPS lifetime database
./data/interim/harps_lifetime_database.csv: ./src/scripts/pre-processing/extract_harps_lifetimes.py $(UPDATED_SWAN) ./data/processed/cmesrc_BBOXES.db
	@python3 $<

## Parse LASCO CME Database
./data/interim/lasco_cme_database.csv: ./src/scripts/pre-processing/parse_lasco_cme_catalogue.py ./data/raw/lasco/univ_all.txt
	@python3 $<

# Temporally matching HARPS
./data/interim/temporal_matching_harps_database.csv: ./src/scripts/spatiotemporal_matching/temporal_matching.py ./data/interim/lasco_cme_database.csv ./data/interim/harps_lifetime_database.csv ./src/cmesrc/classes.py ./src/harps/harps.py ./src/cmes/cmes.py
	@python3 $<

# Spatiotemporally matching HARPS
./data/interim/spatiotemporal_matching_harps_database.csv: ./src/scripts/spatiotemporal_matching/spatial_matching.py ./data/interim/temporal_matching_harps_database.csv
	@python3 $<

# Match dimmings
./data/interim/dimmings_matched_to_harps.csv: ./src/scripts/dimmings/match_dimmings_to_harps.py ./data/interim/spatiotemporal_matching_harps_database.csv ./src/dimmings/dimmings.py ./data/raw/dimmings/dimmings.csv
	@python3 $<

# Match flares
./data/interim/flares_matched_to_harps.csv: ./src/scripts/flares/match_flares_to_harps.py ./data/interim/spatiotemporal_matching_harps_database.csv ./src/flares/flares.py $(ORIG_SWAN)
	@python3 $<

# Generate catalogue
./data/processed/cmesrc.db: ./src/scripts/catalogue/generate_catalogue.py ./data/interim/spatiotemporal_matching_harps_database.csv ./data/interim/dimmings_matched_to_harps.csv ./data/interim/flares_matched_to_harps.csv ./data/processed/cmesrc_BBOXES.db
	@python3 $<

#################################################################################
# COMMANDS                                                                      #
#################################################################################

## Install requirements and set up virtual environment
create_environment:
	@echo "Checking if pip is installed..."
	@command -v pip > /dev/null 2>&1 || { echo >&2 "pip is required but it's not installed. Aborting."; exit 1; }
	@echo "Checking if venv is installed..."
	@python3 -m venv --help > /dev/null 2>&1 || { echo >&2 "venv is required but it's not installed. Installing venv..."; python3 -m pip install virtualenv; }
	@echo "Creating virtual environment in env folder..."
	@python3 -m venv env
	@echo "Virtual environment set up successfully. Activate with 'source env/bin/activate'."

install_requirements:
	@echo "Installing requirements from requirements.txt..."
	@pip install -r requirements.txt
	@	echo "Requirements installed successfully."

#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := help

# Inspired by <http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html>
# sed script explained:
# /^##/:
# 	* save line in hold space
# 	* purge line
# 	* Loop:
# 		* append newline + line to hold space
# 		* go to next line
# 		* if line starts with doc comment, strip comment character off and loop
# 	* remove target prerequisites
# 	* append hold space (+ newline) to line
# 	* replace newline plus comments by `---`
# 	* print line
# Separate expressions are necessary because labels cannot be delimited by
# semicolon; see <http://stackoverflow.com/a/11799865/1968>
.PHONY: help
help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)"
	@echo
	@sed -n -e "/^## / { \
		h; \
		s/.*//; \
		:doc" \
		-e "H; \
		n; \
		s/^## //; \
		t doc" \
		-e "s/:.*//; \
		G; \
		s/\\n## /---/; \
		s/\\n/ /g; \
		p; \
	}" ${MAKEFILE_LIST} \
	| LC_ALL='C' sort --ignore-case \
	| awk -F '---' \
		-v ncol=$$(tput cols) \
		-v indent=19 \
		-v col_on="$$(tput setaf 6)" \
		-v col_off="$$(tput sgr0)" \
	'{ \
		printf "%s%*s%s ", col_on, -indent, $$1, col_off; \
		n = split($$2, words, " "); \
		line_length = ncol - indent; \
		for (i = 1; i <= n; i++) { \
			line_length -= length(words[i]) + 1; \
			if (line_length <= 0) { \
				line_length = ncol - indent - length(words[i]) - 1; \
				printf "\n%*s ", -indent, " "; \
			} \
			printf "%s ", words[i]; \
		} \
		printf "\n"; \
	}' \
	| more $(shell test $(shell uname) = Darwin && echo '--no-init --raw-control-chars')
