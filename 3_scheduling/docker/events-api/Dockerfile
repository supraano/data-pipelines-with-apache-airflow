FROM python:3.8-slim

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && rm -f /tmp/requirements.txt
RUN pip uninstall -y Werkzeug && pip install Werkzeug==2.3.6

COPY app.py /

EXPOSE 5000

ENTRYPOINT ["python"]
CMD ["/app.py"]