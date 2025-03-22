from fastapi.testclient import TestClient
from main import app
from api.commercial.function import run_query

async def test_create_employees_table():
    query = """CREATE TABLE employees (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department_id INT,
    salary DECIMAL(10, 2),
    hire_date DATE
    );"""

    result = await run_query(query)
    assert result == {
        "success": True,
        "result": None,
        "message": "Created Table employees"
    }

async def test_create_departments_table():
    query = """CREATE TABLE departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(100),
    location VARCHAR(100)
    );"""

    result = await run_query(query)
    assert result == {
        "success": True,
        "result": None,
        "message": "Created Table departments"
    }

async def test_insert_data_into_departments_table():
    query = """INSERT INTO departments (department_name, location)
    VALUES ('Engineering', 'New York'),
    ('Marketing', 'San Francisco'),
    ('HR', 'Los Angeles');"""

    result = await run_query(query)
    assert result == {
        "success": True,
        "result": None,
        "message": "Inserted into Table departments"
    }

async def test_insert_data_into_employees_table():
    query = """INSERT INTO employees (first_name, last_name, department_id, salary, hire_date)
    VALUES ('John', 'Doe', 1, 80000.00, '2023-05-20'),
    ('Jane', 'Smith', 2, 95000.00, '2022-09-15'),
    ('Emily', 'Johnson', 1, 72000.00, '2021-11-03'),
    ('Michael', 'Brown', 3, 60000.00, '2020-01-10');"""

    result = await run_query(query)
    assert result == {
        "success": True,
        "result": None,
        "message": "Inserted into Table employees"
    }

async def test_select_data_from_employees_table():
    query = """SELECT e.first_name, e.last_name, e.salary, d.department_name
    FROM employees e
    JOIN departments d ON e.department_id = d.department_id;"""

    result = await run_query(query)
    assert result == {
        "success": True,
        "result": {
            "rows": [
                {
                    "first_name": "John",
                    "last_name": "Doe", 
                    "salary": 80000.12,
                    "department_name": "Engineering"
                },
                {
                    "first_name": "Jane",
                    "last_name": "Smith",
                    "salary": 95000,
                    "department_name": "Marketing"
                },
                {
                    "first_name": "Emily", 
                    "last_name": "Johnson",
                    "salary": 72000,
                    "department_name": "Engineering"
                },
                {
                    "first_name": "Michael",
                    "last_name": "Brown",
                    "salary": 60000,
                    "department_name": "HR"
                }
            ]
        },
        "message": "Selected from Table employees"
    }
