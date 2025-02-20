import pytest
import os
import pandas as pd
from transformer.controller import Csvgres

@pytest.fixture
def db():
    """Fixture to create a test database instance"""
    test_dir = 'test_data'
    db = Csvgres(test_dir)
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
    assert os.path.isdir(os.path.join(db.base_dir, "testdb"))
    
    # Test creating duplicate database
    with pytest.raises(ValueError):
        await db.create_database("CREATE DATABASE testdb")

@pytest.mark.asyncio
async def test_create_table(db):
    # Setup
    await db.create_database("CREATE DATABASE testdb")
    
    # Test creating a table
    create_stmt = """
    CREATE TABLE users (
        id INT,
        name TEXT,
        age INT
    )
    """
    await db.create_table(create_stmt, 'testdb')
    
    table_path = os.path.join(db.base_dir, "testdb", "tables", "users.csv")
    metadata_path = os.path.join(db.base_dir, "testdb", ".metadata", "users.json")
    
    assert os.path.exists(table_path)
    assert os.path.exists(metadata_path)
    
    # Verify metadata contains table info
    import json
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
        assert "columns" in metadata
        expected_columns = {
            "id": {"type": "INT"},
            "name": {"type": "TEXT"},
            "age": {"type": "INT"}
        }
        assert metadata["columns"] == expected_columns
    
    # Verify column structure
    df = pd.read_csv(table_path)
    assert list(df.columns) == ["id", "name", "age"]

@pytest.mark.asyncio
async def test_insert_and_alter_and_select(db):
    # Setup
    await db.create_database("CREATE DATABASE testdb")
    await db.create_table("CREATE TABLE users (id INT, name TEXT, age INT)", 'testdb')
    
    # Test insert
    insert_stmt = "INSERT INTO users VALUES (1, 'John', 30)"
    await db.insert(insert_stmt, 'testdb')
    
    # Test select all
    result = await db.select("SELECT * FROM users", 'testdb')
    assert isinstance(result, list)
    assert len(result) == 1
    assert result == [{'age': 30, 'id': 1, 'name': 'John'}]

    # Test alter
    await db.update_row("UPDATE users SET name = 'Jacob' WHERE name = 'John'", 'testdb')
    result = await db.select("SELECT * FROM users", 'testdb')
    assert isinstance(result, list)
    assert len(result) == 1
    assert result == [{'age': 30, 'id': 1, 'name': 'Jacob'}]
    
    # Test select with where clause
    result = await db.select("SELECT name, age FROM users WHERE age = 30", 'testdb')
    assert isinstance(result, list)
    assert len(result) == 1
    assert result == [{'age': 30, 'name': 'Jacob'}]

@pytest.mark.asyncio
async def test_delete_row(db):
    # Setup
    await db.create_database("CREATE DATABASE testdb")
    await db.create_table("CREATE TABLE users (id INT, name VARCHAR)", 'testdb')
    await db.insert("INSERT INTO users VALUES (1, 'John')", 'testdb')
    await db.insert("INSERT INTO users VALUES (2, 'Jane')", 'testdb')
    
    # Test delete with where clause
    await db.delete_row("DELETE FROM users WHERE id = 1", 'testdb')
    result = await db.select("SELECT * FROM users", 'testdb')
    assert len(result) == 1
    assert result[0]["name"] == "Jane"

@pytest.mark.asyncio
async def test_drop_table(db):
    # Setup
    await db.create_database("CREATE DATABASE testdb")
    await db.create_table("CREATE TABLE users (id INT)", 'testdb')
    
    metadata_path = os.path.join(db.base_dir, "testdb", ".metadata", "users.json")
    
    # Verify table exists in metadata before dropping
    assert os.path.exists(metadata_path)
    
    # Test dropping table
    await db.drop_table("DROP TABLE users", 'testdb')
    table_path = os.path.join(db.base_dir, "testdb", "tables", "users.csv")
    table_metadata_path = os.path.join(db.base_dir, "testdb", ".metadata", "users.json")
    assert not os.path.exists(table_path)
    assert not os.path.exists(table_metadata_path)

@pytest.mark.asyncio
async def test_drop_database(db):
    # Setup
    await db.create_database("CREATE DATABASE testdb")
    
    # Test dropping database
    await db.drop_database("DROP DATABASE testdb")
    assert not os.path.exists(os.path.join(db.base_dir, "testdb"))
    