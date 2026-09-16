FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py ./
COPY ACTIVE_TREND_60D_PAPER.json ./
RUN python -c "from trend_paper_activation import load_active_definitions; assert len(load_active_definitions()) == 10, 'Ten qualified PAPER definitions must be packaged'"
ENV PYTHONUNBUFFERED=1
CMD ["sh","-c","uvicorn server:app --host 0.0.0.0 --port ${PORT:-8000}"]
