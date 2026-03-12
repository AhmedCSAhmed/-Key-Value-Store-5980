FROM golang:1.25-bookworm

WORKDIR /-Key-Value-Store-5980


# Install Python dependencies and netcat for server management
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    git \
    netcat-openbsd \ 
    && rm -rf /var/lib/apt/lists/*

# Copy go modules and run dependencies
COPY go.mod go.sum ./
RUN go mod download

# Add Python Dependencies
COPY requirements.txt .

RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt


# Adds the source code to the docker container
COPY . .  

# Runs script to start server and benchmark testing for kv-store
RUN chmod +x startup_script.sh

CMD [ "bash", "startup_script.sh" ]