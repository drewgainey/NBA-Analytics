FROM python:3.8-slim

RUN apt-get update && apt-get install -y postgresql-client

# Copy the requirements.txt file and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .
# Command to run the script
CMD ["python", "./player_statistics.py"]
