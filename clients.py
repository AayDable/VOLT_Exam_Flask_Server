import os
from openai import AsyncOpenAI
from dotenv import load_dotenv
from data_retrieval import MongoData

load_dotenv()

mongo_data = MongoData()
openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY")) 
