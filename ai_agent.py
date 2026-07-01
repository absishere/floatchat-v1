# ai_agent.py (Refactored for Streamlit with Gemini)

import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent
from langchain_google_genai import ChatGoogleGenerativeAI

# --- Load Environment Variables ---
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# --- Database Configuration ---
DB_USER = 'postgres'
DB_PASSWORD = '123456'  # Your correct password
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'argo_db'
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def initialize_agent():
    """
    Initializes and returns the Gemini-powered SQL agent.
    This function will be imported by our Streamlit app.
    """
    if not GOOGLE_API_KEY:
        raise ValueError(
            "GOOGLE_API_KEY is missing. Add a valid Gemini API key to the .env file."
        )

    print("Initializing AI Agent with Google Gemini...")

    # Initialize the database connection
    engine = create_engine(DATABASE_URL)
    db = SQLDatabase(engine)

    # Initialize the Gemini Pro language model
    # Note: Use a stable model name like 'gemini-2.5-pro' for best results
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        temperature=0,
        google_api_key=GOOGLE_API_KEY,
    )

    # Create the SQL Agent.
    # We remove `agent_type` to let LangChain use the default ReAct agent,
    # which is more compatible with non-OpenAI models.
    agent_executor = create_sql_agent(llm, db=db, agent_type="tool-calling", verbose=True)
    
    print("Gemini agent is ready.")
    return agent_executor

def run_gemini_query(user_question, agent_executor):
    """
    Takes a user question and the agent and returns the AI's answer.
    """
    # The agent is already powerful enough, so we pass the question directly.
    # We add error handling for robustness.
    result = agent_executor.invoke(
        {"input": user_question},
        {"handle_parsing_errors": True}
    )
    
    return result["output"]

# Note: The old `main` function with the `while` loop has been removed.
