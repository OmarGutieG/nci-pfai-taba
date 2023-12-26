# Data Importer

This Python script can import data from a CSV file into either a MySQL or SQLite database. It connects to the specified database, downloads CSV data from a given URL, parses it using pandas, and stores the data in the selected database. The script can work with both MySQL and SQLite databases.

## Setup

### Prerequisites

- Python 3.x
- Required Python packages (install using `pip install -r requirements.txt`):
  - `requests`
  - `pandas`
  - `sqlalchemy`
  - `mysql-connector-python`

### Usage
1. Run the script in a Python environment.
2. The script will download CSV data from the specified URL, parse it, and store it in either the MySQL or SQLite database.
3. The retrieved data will be displayed.

### Additional Information
• The script uses SQLAlchemy to handle database connections.
• CSV data is downloaded from the following URL: CSV Data.
• For MySQL, a database named taba_database will be used, and the table will be named lacity.
• For SQLite, a file named taba_database.db will be created in the script's directory, and the table will be named lacity.


### Configuration

Modify the following variables in the script to match your database configuration:

´´´python
# For MySQL
user_name = "root"
password = ""
host = "localhost"
port = "3306"
target_database = 'taba_database'
table_name_mysql = "lacity"

# For SQLite
table_name_sqlite = "lacity"