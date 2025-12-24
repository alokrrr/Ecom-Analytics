import os
from dotenv import load_dotenv
load_dotenv()
print("CWD:", os.getcwd())
print("DATABASE_URL (masked):", ("<not set>" if not os.getenv('DATABASE_URL') else "<set>"))