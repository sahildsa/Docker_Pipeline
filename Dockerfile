#--- Creating the docker file as stages ---
#--- First stage take csv file use pandas(can use any thing Apache spark and Beam for distributed systems) and clean it ---

FROM python:3.9-slim as builder

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY input.csv .

COPY process_data.py .

RUN python process_data.py .

# --- Second step we will now upload the data into our Postgre Database --- 

FROM python:3.9-slim

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages

COPY --from=builder /app/output.csv .

COPY upload_data.py .

CMD ["python", "upload_data.py"]



