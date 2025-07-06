import csv
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import create_engine, text

from splurge_data_profiler.data_lake import DataLakeFactory
from splurge_data_profiler.profiler import Profiler
from splurge_data_profiler.source import DsvSource, Column

EXAMPLES_DIR = Path(__file__).parent
DSV_PATH = EXAMPLES_DIR / "example_data.dsv"
DB_PATH = EXAMPLES_DIR

# Sample data for generating realistic records
FIRST_NAMES = [
    "Alice", "Bob", "Charlie", "Dana", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack",
    "Kate", "Liam", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Ruby", "Sam", "Tara",
    "Uma", "Victor", "Wendy", "Xavier", "Yara", "Zoe", "Adam", "Bella", "Chris", "Diana",
    "Ethan", "Fiona", "George", "Hannah", "Ian", "Julia", "Kevin", "Laura", "Mike", "Nina",
    "Oscar", "Penny", "Ryan", "Sarah", "Tom", "Ursula", "Vince", "Willa", "Xander", "Yuki"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts"
]

CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio",
    "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville", "Fort Worth", "Columbus",
    "Charlotte", "San Francisco", "Indianapolis", "Seattle", "Denver", "Washington",
    "Boston", "El Paso", "Nashville", "Detroit", "Oklahoma City", "Portland", "Las Vegas",
    "Memphis", "Louisville", "Baltimore", "Milwaukee", "Albuquerque", "Tucson", "Fresno",
    "Sacramento", "Mesa", "Kansas City", "Atlanta", "Long Beach", "Colorado Springs",
    "Raleigh", "Miami", "Virginia Beach", "Omaha", "Oakland", "Minneapolis", "Tampa",
    "Tulsa", "Arlington", "New Orleans", "Wichita"
]

DEPARTMENTS = [
    "Engineering", "Marketing", "Sales", "HR", "Legal", "Finance", "Operations",
    "Customer Support", "Product Management", "Research & Development", "IT",
    "Business Development", "Quality Assurance", "Design", "Data Science"
]

# Generate example DSV file with 7500 rows, | delimiter, and " bookend
def generate_dsv(file_path: Path, *, num_rows: int = 7500, delimiter: str = "|", bookend: str = '"') -> None:
    """
    Generate a DSV file with the specified number of rows, delimiter, and bookend.
    Args:
        file_path: Path where the DSV file will be created
        num_rows: Number of rows to generate (default: 7500)
        delimiter: Delimiter character (default: |)
        bookend: Bookend/quote character (default: ")
    """
    columns = [
        "id", "name", "email", "age", "city", "salary", "department", "hire_date",
        "score", "last_login", "shift_start", "is_active"
    ]
    start_date = datetime(2015, 1, 1)
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=delimiter, quotechar=bookend, quoting=csv.QUOTE_ALL)
        writer.writerow(columns)
        for i in range(1, num_rows + 1):
            first_name = random.choice(FIRST_NAMES)
            last_name = random.choice(LAST_NAMES)
            name = f"{first_name} {last_name}"
            email = f"{first_name.lower()}.{last_name.lower()}@example.com"
            age = random.randint(21, 65)
            city = random.choice(CITIES)
            salary = random.randint(40000, 180000)
            department = random.choice(DEPARTMENTS)
            hire_date = (start_date + timedelta(days=random.randint(0, 365 * 8))).date().isoformat()
            score = round(random.uniform(0, 100), 2)
            last_login = (
                start_date + timedelta(
                    days=random.randint(0, 365 * 8), 
                    hours=random.randint(0, 23), 
                    minutes=random.randint(0, 59)
                )
            ).isoformat(sep="T", timespec="seconds")
            shift_start = f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
            is_active = random.choice(["true", "false"])
            row = [
                str(i), name, email, str(age), city, str(salary), department, hire_date,
                str(score), last_login, shift_start, is_active
            ]
            writer.writerow(row)

# Define DsvSource columns
COLUMNS = [
    Column(name="id"),
    Column(name="name"),
    Column(name="email"),
    Column(name="age"),
    Column(name="city"),
    Column(name="salary"),
    Column(name="department"),
    Column(name="hire_date"),
]

def main() -> None:
    # Generate DSV with 7500 rows, | delimiter, and " bookend
    print("Generating DSV with 7500 rows of realistic data...")
    generate_dsv(DSV_PATH, num_rows=7500, delimiter="|", bookend='"')
    print(f"DSV file created at: {DSV_PATH}")

    # Remove old SQLite database tables if they exist using SQLAlchemy
    db_filename = "example_data.sqlite"
    DB_PATH = EXAMPLES_DIR / db_filename
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db_url = f"sqlite:///{DB_PATH}"
    engine = create_engine(db_url)
    with engine.connect() as connection:
        for table in ["example_data_inferred", "example_data"]:
            connection.execute(text(f"DROP TABLE IF EXISTS {table}"))
    engine.dispose()

    # Create DsvSource
    dsv_source = DsvSource(
        file_path=str(DSV_PATH),
        delimiter="|",
        bookend='"'
    )

    # Create/overwrite database
    print("Creating database from DSV...")
    datalake = DataLakeFactory.from_dsv_source(
        dsv_source=dsv_source,
        data_lake_path=EXAMPLES_DIR
    )
    print(f"Database created at: {datalake.db_url}")
    print(f"Table name: {datalake.db_table}")

    # Create profiler and profile the data
    profiler = Profiler(data_lake=datalake)
    profiler.profile()  # Uses adaptive sampling by default
    
    # Create inferred table
    inferred_table_name = profiler.create_inferred_table()
    print(f"Inferred table created: {inferred_table_name}")
    
    # Show profiling results
    print("\nProfiling Results:")
    for column in profiler.profiled_columns:
        print(f"  {column.name}: {column.inferred_type.value}")

if __name__ == "__main__":
    main() 