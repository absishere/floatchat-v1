# app.py

import streamlit as st
# Import the refactored functions from your Gemini agent script
from ai_agent import initialize_agent, run_gemini_query 

# --- App Configuration ---
st.set_page_config(
    page_title="FloatChat (Gemini Edition)",
    page_icon="🌊",
    layout="wide"
)

st.title("🌊 FloatChat - AI Explorer for ARGO Ocean Data")
st.caption("Powered by Google Gemini Pro")

# --- Agent Initialization ---
# Use st.cache_resource to initialize the agent only once
@st.cache_resource
def load_agent():
    return initialize_agent()

agent_executor = load_agent()

# --- Chat History Management ---
if "messages" not in st.session_state:
    st.session_state.messages = [{
        "role": "assistant", 
        "content": "Hello! I'm FloatChat. Ask me about the ARGO data in the database."
    }]

# Display chat messages from history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# --- Chat Input and Response ---
if prompt := st.chat_input("What is the average salinity?"):
    # Add user message to history and display it
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Get and display assistant response
    with st.chat_message("assistant"):
        # Show a thinking spinner while the agent works
        with st.spinner("Querying the database with Gemini..."):
            response = run_gemini_query(prompt, agent_executor)
            st.markdown(response)
    
    # Add assistant response to history
    st.session_state.messages.append({"role": "assistant", "content": response})