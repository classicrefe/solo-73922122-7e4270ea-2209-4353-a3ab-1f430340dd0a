FROM python:3.12-slim

WORKDIR /app
COPY scpi_simulator.py .

ENV SIM_HOST=0.0.0.0 \
    SIM_PORT=5025 \
    SIM_WAVEFORM=sine \
    SIM_SEGMENT=1024

EXPOSE 5025
CMD ["python", "scpi_simulator.py"]
