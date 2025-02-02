import pytest
import os
import pandas as pd
import asyncio
from utils.csv_database import CsvDatabase

@pytest.fixture
def db():
    """Fixture to create a test database instance"""
    test_dir = 'test_data'
    db = CsvDatabase(test_dir)
    db.init()
    yield db
    # Cleanup after tests
    if os.path.exists(test_dir):
        import shutil
        shutil.rmtree(test_dir)

@pytest.mark.asyncio
async def test_create_database(db):
    # Test creating a new database
    await db.create_database("CREATE DATABASE testdb")
    assert os.path.exists(os.path.join(db.base_dir, "testdb"))
    
    # Test creating duplicate database
    with pytest.raises(ValueError):
        await db.create_database("CREATE DATABASE testdb")

@pytest.mark.asyncio
async def test_connect_database(db):
    # Create a database first
    await db.create_database("CREATE DATABASE testdb")
    
    # Test valid connection
    await db.connect_database(r"\c testdb")
    assert db.current_database == "testdb"
    
    # Test invalid database name
    with pytest.raises(ValueError):
        await db.connect_database(r"\c nonexistent")
    
    # Test invalid connect command
    with pytest.raises(ValueError):
        await db.connect_database("invalid command")

@pytest.mark.asyncio
async def test_create_table(db):
    # Setup
    await db.create_database("CREATE DATABASE testdb")
    await db.connect_database(r"\c testdb")
    
    # Test creating a table
    create_stmt = """
    CREATE TABLE users (
        id INT,
        name VARCHAR,
        age INT
    )
    """
    await db.create_table(create_stmt)
    
    table_path = os.path.join(db.base_dir, "testdb", "users.csv")
    assert os.path.exists(table_path)
    
    # Verify column structure
    df = pd.read_csv(table_path)
    assert list(df.columns) == ["id", "name", "age"]

@pytest.mark.asyncio
async def test_insert_and_select(db):
    # Setup
    await db.create_database("CREATE DATABASE testdb")
    await db.connect_database(r"\c testdb")
    await db.create_table("CREATE TABLE users (id INT, name VARCHAR, age INT)")
    
    # Test insert
    insert_stmt = "INSERT INTO users VALUES (1, 'John', 30)"
    await db.insert(insert_stmt)
    
    # Test select all
    result = await db.select("SELECT * FROM users")
    assert len(result) == 1
    assert result.iloc[0]["name"] == "John"
    
    # Test select with where clause
    result = await db.select("SELECT name, age FROM users WHERE age = 30")
    assert len(result) == 1
    assert list(result.columns) == ["name", "age"]

@pytest.mark.asyncio
async def test_delete_row(db):
    # Setup
    await db.create_database("CREATE DATABASE testdb")
    await db.connect_database(r"\c testdb")
    await db.create_table("CREATE TABLE users (id INT, name VARCHAR)")
    await db.insert("INSERT INTO users VALUES (1, 'John')")
    await db.insert("INSERT INTO users VALUES (2, 'Jane')")
    
    # Test delete with where clause
    await db.delete_row("DELETE FROM users WHERE id = 1")
    result = await db.select("SELECT * FROM users")
    assert len(result) == 1
    assert result.iloc[0]["name"] == "Jane"

@pytest.mark.asyncio
async def test_drop_table(db):
    # Setup
    await db.create_database("CREATE DATABASE testdb")
    await db.connect_database(r"\c testdb")
    await db.create_table("CREATE TABLE users (id INT)")
    
    # Test dropping table
    await db.drop_table("DROP TABLE users")
    table_path = os.path.join(db.base_dir, "testdb", "users.csv")
    assert not os.path.exists(table_path)

@pytest.mark.asyncio
async def test_drop_database(db):
    # Setup
    await db.create_database("CREATE DATABASE testdb")
    await db.create_database("CREATE DATABASE testdb2")
    await db.connect_database(r"\c testdb2")
    
    # Test dropping database
    await db.drop_database("DROP DATABASE testdb")
    assert not os.path.exists(os.path.join(db.base_dir, "testdb"))
    
    # Test cannot drop connected database
    with pytest.raises(ValueError):
        await db.drop_database("DROP DATABASE testdb2") 