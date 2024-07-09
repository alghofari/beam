# Use a Java image as the base
FROM openjdk:17-jdk

# Set the working directory
WORKDIR /app

# # Install xargs (from findutils)
# RUN apt-get update && apt-get install -y findutils

# Copy config files
COPY gradlew gradlew
COPY gradle gradle

# Copy the Gradle wrapper and build configuration
COPY . .

# Download dependencies
#Build without Gradle daemon to avoid issues in container
RUN ./gradlew build
