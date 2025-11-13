FROM public.ecr.aws/lambda/python:3.12

WORKDIR /var/task

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/
COPY pyproject.toml .
RUN uv sync --no-dev --python=/var/lang/bin/python3.12

ENV PYTHONPATH="/var/task/.venv/lib/python3.12/site-packages:${PYTHONPATH}"

COPY handler.py .

CMD ["handler.greyscale_handler"]