FROM node:18

# Install Python + venv + pip
RUN apt-get update && apt-get install -y python3 python3-venv python3-pip

# Set working directory
WORKDIR /app

# Copy package files and install Node dependencies
COPY package*.json ./
RUN npm install

# Copy rest of app
COPY . .

# Create virtual environment and install Python packages
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --upgrade pip
RUN pip install psycopg2-binary pandas

# Default command
CMD ["npm", "start"]
