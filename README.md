# Apache Beam Project

## Overview
This project demonstrates the use of Apache Beam, a unified stream and batch processing model for big data processing. The project showcases the implementation of data processing pipelines using Apache Beam's Java SDK.

## Table of Contents
- [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [Installation](#installation)
- [Usage](#usage)

## Getting Started

### Prerequisites
Before you begin, ensure you have the following prerequisites installed:

- Java Development Kit (JDK) 8 or higher
- Gradle

### Installation
1. Clone the repository:

   ```bash
   git clone ssh://git@phabricator.sirclo.com:2222/diffusion/521/beam.git
   ```
2. Navigate to the project directory:

   ```bash
   cd beam
   ```
3. Build the project using Gradle:

   ```bash
   ./gradlew build
   ```

### Usage
```bash
./gradlew run --args="--projectId=sirclo-prod --subscriptionName=cnx_order_process_log-osiris-sub"
```