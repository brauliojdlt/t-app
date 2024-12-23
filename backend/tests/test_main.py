import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from main import app, get_db, Base
from models import Transaction
from datetime import datetime

# Test database setup
DATABASE_URL = "sqlite:///./:memory:"  # Use SQLite for testing
engine = create_engine(DATABASE_URL)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Override the `get_db` dependency
def override_get_db():
    db = TestingSessionLocal()

    try:
        yield db
    finally:
        db.close()

app.dependency_overrides[get_db] = override_get_db

# Create test client
client = TestClient(app)


@pytest.fixture(scope="function", autouse=True)
def setup_and_teardown():
    """Set up an in-memory database for each test."""
    # Create tables before each test
    Base.metadata.create_all(bind=engine)

    yield  # Run the test

    # Drop all tables after the test (optional for in-memory DB)
    Base.metadata.drop_all(bind=engine)

@pytest.fixture
def setup_test_data():
    """Fixture to set up initial test data."""
    db = TestingSessionLocal()
    test_transactions = [
        Transaction(
            transaction_id="TX_001",
            customer_id="CUST_001",
            card_number="4111111111111111",
            timestamp=datetime.now(),
            merchant_category="Retail",
            merchant_type="Online",
            merchant="Amazon",
            amount=100.0,
            currency="USD",
            country="USA",
            city="New York",
            city_size="large",
            card_type="Gold Credit",
            card_present=False,
            device="Chrome",
            channel="web",
            device_fingerprint="abc123",
            ip_address="192.168.1.1",
            distance_from_home=True,
            high_risk_merchant=False,
            transaction_hour=12,
            weekend_transaction=False,
            velocity_last_hour={"num_transactions": 5, "total_amount": 500.0},
            is_fraud=False,
        )
    ]
    db.add_all(test_transactions)
    db.commit()
    db.close()


@pytest.mark.usefixtures("setup_test_data")
def test_get_transactions():
    """Test fetching transactions with pagination."""
    response = client.get("/transactions?page=1&page_size=10")
    assert response.status_code == 200
    data = response.json()
    assert "transactions" in data
    assert "metadata" in data
    assert data["metadata"]["total_records"] == 1  # Based on test data
    assert len(data["transactions"]) == 1  # Only 1 transaction in test data

@pytest.mark.usefixtures("setup_test_data")
def test_get_transaction_by_id():
    """Test fetching a single transaction by ID."""
    response = client.get("/transactions/TX_001")
    assert response.status_code == 200
    data = response.json()
    assert data["transaction_id"] == "TX_001"
    assert data["merchant"] == "Amazon"


def test_get_transaction_not_found():
    """Test fetching a non-existent transaction."""
    response = client.get("/transactions/TX_999")
    assert response.status_code == 404
    data = response.json()
    assert data["detail"] == "Transaction not found"
