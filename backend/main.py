from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.sql.expression import cast
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from models import Transaction, Base
from datetime import datetime
from typing import Optional
import json

import logging
from fastapi import Request
from fastapi.responses import JSONResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("server.log"),  # Log to a file
        logging.StreamHandler()            # Log to console
    ]
)
logger = logging.getLogger(__name__)

# SQLAlchemy setup
# DATABASE_URL = "sqlite:///./test.db"
DATABASE_URL = "postgresql://newuser:mypassword@localhost:5432/mydatabase"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)



# Create tables
Base.metadata.create_all(bind=engine)



class TransactionResponse(BaseModel):
    transaction_id: str
    customer_id: str
    card_number: str
    timestamp: datetime
    merchant_category: str
    merchant_type: str
    merchant: str
    amount: float
    currency: str
    country: str
    city: str
    city_size: Optional[str]
    card_type: Optional[str]
    card_present: Optional[bool]
    device: Optional[str]
    channel: Optional[str]
    device_fingerprint: Optional[str]
    ip_address: Optional[str]
    distance_from_home: Optional[bool]
    high_risk_merchant: Optional[bool]
    transaction_hour: int
    weekend_transaction: Optional[bool]
    velocity_last_hour: Optional[dict]  # Represented as a dictionary
    is_fraud: bool

    class Config:
        orm_mode = True

class TransactionsResponse(BaseModel):
    transactions: list[TransactionResponse]
    metadata: dict

# FastAPI setup
app = FastAPI()


@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Incoming request: {request.method} {request.url} ")
    response = await call_next(request)
    logger.info(f"Response status: {response.status_code}")
    return response

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"message": "An internal error occurred."}
    )

# Dependency to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        
        
from fastapi import FastAPI, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional
from main import Transaction, TransactionsResponse, get_db

app = FastAPI()

@app.get("/transactions/search", response_model=TransactionsResponse)
def search_transactions(
    db: Session = Depends(get_db),
    merchant: Optional[str] = Query(None, description="Search by merchant name"),
    min_amount: Optional[float] = Query(None, description="Search by minimum amount"),
    max_amount: Optional[float] = Query(None, description="Search by maximum amount"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
):
    """
    Search for transactions by merchant or amount range.
    """
    query = db.query(Transaction)

    # Apply filters based on query parameters
    if merchant:
        query = query.filter(Transaction.merchant.ilike(f"%{merchant}%"))
    if min_amount is not None:
        query = query.filter(Transaction.amount >= min_amount)
    if max_amount is not None:
        query = query.filter(Transaction.amount <= max_amount)

    print(query)
    # Pagination
    offset = (page - 1) * page_size
    total_records = query.count()
    transactions = query.offset(offset).limit(page_size).all()

    return {
        "transactions": transactions,
        "metadata": {"total_records": total_records},
    }




@app.get("/transactions", response_model=TransactionsResponse)
def get_transactions(
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),           # Page must be at least 1
    page_size: int = Query(50, ge=1, le=100),  # Maximum 100 records per page
):
    # Calculate the offset based on page and page_size
    offset = (page - 1) * page_size

    # Query the database with limit and offset
    try:
        transactions = (
            db.query(
                Transaction
            )
            .order_by(Transaction.timestamp.desc())
            .offset(offset)
            .limit(page_size)
            .all()
        )

        total_records = db.query(Transaction).count()

        return {
            "transactions": transactions,
            "metadata":{
                "total_records": total_records
            }
        }
    except Exception as e:
        print(f"Error getting transactions {str(e)}")
        raise e
        

@app.get("/transactions/{transaction_id}", response_model=TransactionResponse)
def get_transaction(transaction_id: str, db: Session = Depends(get_db)):
    transaction = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
    if not transaction:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return transaction