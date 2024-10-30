import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from the .env file

azure_endpoint = "https://uswest3daniel.openai.azure.com"
api_key = "fcb2ce5dc289487fad0f6674a0b35312"
api_version = "2024-10-01-preview"
model = "GPT-4Omni"
azure_function_url = "https://doc2pdf.azurewebsites.net/api/HttpTrigger1"
