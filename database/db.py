import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv('C:/Projects/meridian/.env')

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5433')
DB_NAME = os.getenv('DB_NAME', 'meridian')
DB_USER = os.getenv('DB_USER', 'meridian_user')
DB_PASS = os.getenv('DB_PASS', 'meridian_pass')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)

def get_engine():
    return engine

def get_session():
    return SessionLocal()

def test_connection():
    try:
        with engine.connect() as conn:
            print(f"Connected to {DB_NAME} on {DB_HOST}:{DB_PORT}")
            return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False

if __name__ == "__main__":
    test_connection()