import csv
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import create_engine, text

from splurge_data_profiler.data_lake import DataLakeFactory
from splurge_data_profiler.profiler import Profiler
from splurge_data_profiler.source import DsvSource, Column

EXAMPLES_DIR = Path(__file__).parent
DSV_PATH = EXAMPLES_DIR / "performance_test_data.dsv"

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

def generate_dsv(file_path: Path, *, num_rows: int = 100000, delimiter: str = "|", bookend: str = '"') -> None:
    """
    Generate a DSV file with the specified number of rows, delimiter, and bookend.
    Args:
        file_path: Path where the DSV file will be created
        num_rows: Number of rows to generate (default: 100000)
        delimiter: Delimiter character (default: |)
        bookend: Bookend/quote character (default: ")
    """
    columns = [
        "id", "name", "email", "age", "city", "salary", "department", "hire_date",
        "score", "last_login", "shift_start", "is_active"
    ]
    start_date = datetime(2015, 1, 1)
    
    print(f"Generating {num_rows:,} rows...")
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=delimiter, quotechar=bookend, quoting=csv.QUOTE_ALL)
        writer.writerow(columns)
        
        for i in range(1, num_rows + 1):
            if i % 10000 == 0:
                print(f"  Generated {i:,} rows...")
                
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

def main() -> None:
    # Test different dataset sizes
    dataset_sizes = [10000, 25000, 50000, 100000, 250000, 500000]
    
    for num_rows in dataset_sizes:
        print(f"\n{'='*60}")
        print(f"=== Performance Test: {num_rows:,} Rows ===")
        print(f"{'='*60}")
        
        # Generate DSV with current size
        print(f"Generating DSV with {num_rows:,} rows of realistic data...")
        generate_dsv(DSV_PATH, num_rows=num_rows, delimiter="|", bookend='"')
        print(f"DSV file created at: {DSV_PATH}")

        # Remove old SQLite database tables if they exist using SQLAlchemy
        db_filename = "performance_test_data.sqlite"
        DB_PATH = EXAMPLES_DIR / db_filename
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        db_url = f"sqlite:///{DB_PATH}"
        engine = create_engine(db_url)
        with engine.connect() as connection:
            for table in ["performance_test_data_inferred", "performance_test_data"]:
                connection.execute(text(f"DROP TABLE IF EXISTS {table}"))
        engine.dispose()

        # Create DsvSource
        dsv_source = DsvSource(
            file_path=str(DSV_PATH),
            delimiter="|",
            bookend='"'
        )

        # Create/overwrite database
        print("\nCreating database from DSV...")
        start_time = time.time()
        datalake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=EXAMPLES_DIR
        )
        db_creation_time = time.time() - start_time
        print(f"Database created at: {datalake.db_url}")
        print(f"Table name: {datalake.db_table}")
        print(f"Database creation time: {db_creation_time:.2f} seconds")

        # Create profiler and test adaptive sampling
        print("\n=== Testing Adaptive Sampling ===")
        profiler = Profiler(data_lake=datalake)
        
        # Test adaptive sampling (default)
        print(f"\nTesting adaptive sampling (default):")
        start_time = time.time()
        profiler.profile()  # Uses adaptive sampling by default
        adaptive_profiling_time = time.time() - start_time
        
        print(f"  Adaptive profiling time: {adaptive_profiling_time:.2f} seconds")
        
        # Show profiling results
        print("  Inferred types:")
        for column in profiler.profiled_columns:
            print(f"    {column.name}: {column.inferred_type.value}")
        
        # Create inferred table with timing
        print(f"\n=== Creating Inferred Table ===")
        start_time = time.time()
        inferred_table_name = profiler.create_inferred_table()
        table_creation_time = time.time() - start_time
        print(f"Inferred table created: {inferred_table_name}")
        print(f"Table creation time: {table_creation_time:.2f} seconds")
        
        # Verify row counts
        engine = create_engine(datalake.db_url)
        with engine.connect() as connection:
            # Check original table
            result = connection.execute(text(f"SELECT COUNT(*) FROM {datalake.db_table}"))
            original_count = result.fetchone()[0]
            
            # Check inferred table
            result = connection.execute(text(f"SELECT COUNT(*) FROM {inferred_table_name}"))
            inferred_count = result.fetchone()[0]
        
        engine.dispose()
        
        print(f"\n=== Results for {num_rows:,} rows ===")
        print(f"Original table rows: {original_count:,}")
        print(f"Inferred table rows: {inferred_count:,}")
        print(f"Total processing time: {db_creation_time + adaptive_profiling_time + table_creation_time:.2f} seconds")
        
        print(f"\n{'='*60}")
    
    # Clean up DSV files created during testing
    print(f"\n=== Cleanup ===")
    if DSV_PATH.exists():
        DSV_PATH.unlink()
        print(f"Deleted DSV file: {DSV_PATH}")
    
    # Also clean up the database file
    db_filename = "performance_test_data.sqlite"
    DB_PATH = EXAMPLES_DIR / db_filename
    if DB_PATH.exists():
        DB_PATH.unlink()
        print(f"Deleted database file: {DB_PATH}")
    
    print("Cleanup complete!")

if __name__ == "__main__":
    main() 