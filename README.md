# RelaxXL Database Connector

RelaxXL Database Connector is a Python-based toolkit for managing, analyzing, and visualizing data related to RelaxXL sessions. The project is organized into several modules for data ingestion, database management, exploratory data analysis (EDA), statistical analysis, and results visualization.

## Project Structure

- **0-EDA/**: Scripts for exploratory data analysis and coverage statistics.
- **1-DB/**: Tools for loading data into a database, decorating patient data, and managing research groups. Includes the database schema (`scheme.sql`).
- **2-Analysis/**: Scripts for full coverage analysis and demographic statistics.
- **3-Statistics/**: Statistical analysis scripts, including session and week statistics, SCR (Skin Conductance Response) per minute, and filtering.
- **4-Results/**: Scripts for generating boxplots and performing t-tests on results.
- **RXLDBC/**: Core Python package for connecting to the database and plotting utilities.

## Getting Started

### Prerequisites
- Python 3.8+
- RelaxXL PostgreSQL database
- Recommended: Create a virtual environment

### Database Setup from dump
1. Install PostgreSQL version 17.

[!IMPORTANT]
Ensure you have added PostgreSQL's `bin` directory to your system's PATH.

2. Create a new database:
   ```bash
   createdb relaxxl
   ```
3. Restore the database from the provided dump file:
   ```bash
    pg_restore -U your_username -d relaxxl -Fd -j 4 --verbose "path/to/dump/folder"
    ```

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/niek265/RelaxXL-Database-Connector.git
   cd RelaxXL-Database-Connector
   ```
2. Install required Python packages:
   ```bash
   pip install -r requirements.txt
   ```
   
3. Create an `.env` file in the root directory of the project with your database connection details:
   ```
   DB_NAME=relaxxl
   DB_USER=your_username
   DB_PASSWORD=your_password
   DB_HOST=localhost
   DB_PORT=5432
   ```

### Usage
- Run scripts in the numbered folders for specific tasks (e.g., data loading, EDA, statistics).
- Use the `RXLDBC` package for database connections and plotting.
- CSV files provide precomputed statistics for further analysis or visualization.

## Example
To load data into the database:
```bash
python 1-DB/1-0_Data_to_database.py
```

To generate coverage plots:
```bash
python 0-EDA/0-1_Coverage_plot.py
```




