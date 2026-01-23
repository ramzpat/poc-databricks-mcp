FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -s /bin/bash vscode

# Set working directory
WORKDIR /workspace

# Copy project files
COPY . .

# Change ownership to vscode user
RUN chown -R vscode:vscode /workspace

USER vscode

# Install Python dependencies
RUN pip install --upgrade pip setuptools wheel && \
    pip install -e .[test]

CMD ["/bin/bash"]
