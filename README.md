# Solar Physics Research Project

## Overview
This project focuses on the analysis of solar physics data, specifically related to Coronal Mass Ejections (CMEs), High-resolution Active Region Patches (HARPs), and other solar phenomena. The project involves data processing, analysis, and the generation of a comprehensive catalogue of solar events.

## Project Structure
- **Makefile**: Contains the build instructions and dependencies for the project.
- **src/scripts/catalogue/generate_catalogue.py**: The main script responsible for generating the solar event catalogue.
- **src/scripts/catalogue/generate_catalogue_steps.md**: Detailed steps outlining the process of generating the catalogue.
- **makefile_readme.md**: Documentation for the `make all` command in the Makefile, detailing the steps executed.

## Getting Started

### Prerequisites
- Python 3.x
- Conda (optional but recommended for environment management)
- AWS CLI (if using AWS S3 for data storage)

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/your-repo/solar-physics-project.git
   cd solar-physics-project
   ```

2. Set up the environment:
   - Using Conda:
     ```bash
     conda env create -f environment.yml
     conda activate solar-physics-env
     ```
   - Using pip:
     ```bash
     pip install -r requirements.txt
     ```

3. Configure AWS credentials if using S3 for data storage:
   ```bash
   aws configure
   ```

### Usage
1. Run the main catalogue generation script:
   ```bash
   python src/scripts/catalogue/generate_catalogue.py
   ```

2. Alternatively, use the Makefile to execute the full pipeline:
   ```bash
   make all
   ```

## Makefile Commands
For a detailed breakdown of the `make all` command, refer to [makefile_readme.md](makefile_readme.md).

## Contributing
Contributions are welcome! Please read the [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to contribute to this project.

## License
This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.
