.PHONY: clean lint requirements sync_data_to_s3 sync_data_from_s3

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

.PHONY: all
all: ./data/interim/scored_harps_matching_dimmings_database.csv ./data/interim/scored_harps_matching_flares_database.csv

## Create HARPS lifetime database
./data/interim/harps_lifetime_database.csv: ./src/scripts/pre-processing/extract_harps_lifetimes.py
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

# Temporal matching dimmings
./data/interim/temporal_matching_dimmings_database.csv: ./src/scripts/dimmings/find_temporal_matching_dimmings.py ./data/interim/spatiotemporal_matching_harps_database.csv ./src/dimmings/dimmings.py
	@python3 $<

# Calculate distances and other information related to dimmings
./data/interim/harps_matching_dimmings_database.csv: ./src/scripts/dimmings/gather_harps_dimming_distances.py ./data/interim/temporal_matching_dimmings_database.csv
	@python3 $<

# Score the likelihood of a dimming belonging to a HARPs region and assign them
./data/interim/scored_harps_matching_dimmings_database.csv: ./src/scripts/dimmings/find_matching_dimming_harps.py ./data/interim/harps_matching_dimmings_database.csv
	@python3 $<

# Temporal matching flares
./data/interim/temporal_matching_flares_database.csv: ./src/scripts/flares/find_temporal_matching_flares.py ./data/interim/spatiotemporal_matching_harps_database.csv ./src/flares/flares.py
	@python3 $<

# Calculate distances and other information related to flares
./data/interim/harps_matching_flares_database.csv: ./src/scripts/flares/gather_harps_flares_distances.py ./data/interim/temporal_matching_flares_database.csv
	@python3 $<

# Score the likelihood of a flare belonging to a HARPs region and assign them
./data/interim/scored_harps_matching_flares_database.csv: ./src/scripts/flares/find_matching_flare_harps.py ./data/interim/harps_matching_flares_database.csv
	@python3 $<

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
