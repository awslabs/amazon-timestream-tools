FROM python:3.12-slim@sha256:3d5ed973e45820f5ba5e46bd065bd88b3a504ff0724d85980dcd05eab361fcf4

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY streamlit_app.py ./

EXPOSE 8501
ENTRYPOINT ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.headless=true"]
