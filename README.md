# Hackathon

## Installation
```
git clone https://github.com/masolar/Hackathon --recurse-submodules
```

## Data Generation
```
cd synthea
./run_synthea California -p 10000 -s 10000 -cs 34497
```
This command would generate 10000 files of synthetic patient data.

## Creating a Database for the Data
1. Setup Postgres
2. Create a database called hackathon
3. Run the init_db script

This may take a while, so you might want to use a smaller amount of generated data
