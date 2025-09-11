# ai_agent.py

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent
from langchain_google_genai import ChatGoogleGenerativeAI # <-- IMPORT GEMINI

# --- Load Environment Variables ---
# This now loads the GOOGLE_API_KEY from your .env file
load_dotenv()

# --- Database Configuration ---
DB_USER = 'postgres'
DB_PASSWORD = '123456'  # Your correct password
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'argo_db'

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def main():
    """
    Sets up the AI agent using Google Gemini Pro.
    """
    print("🤖 Initializing AI Agent with Google Gemini Pro...")

    # Initialize the database connection
    engine = create_engine(DATABASE_URL)
    db = SQLDatabase(engine)

    # Initialize the Gemini Pro language model
    # The 'convert_system_message_to_human=True' is a helpful compatibility flag
    # for the SQL agent, which relies heavily on system prompts.
    llm = ChatGoogleGenerativeAI(model="gemini-2.5-pro", temperature=0,
                                 convert_system_message_to_human=True)

    # Create the SQL Agent. The core logic remains the same!
    agent_executor = create_sql_agent(llm, db=db, agent_type="openai-tools", verbose=True)

    print("✅ Gemini agent is ready! Ask your questions about the ARGO data.")
    print("   Type 'exit' to quit.")

    while True:
        try:
            user_question = input("\nYour question: ")
            if user_question.lower() == 'exit':
                break

            # The agent will now process your question
            response = agent_executor.invoke(user_question)

            print("\nAI Response:")
            print(response['output'])

        except Exception as e:
            print(f"\nAn error occurred: {e}")

if __name__ == "__main__":
    main()