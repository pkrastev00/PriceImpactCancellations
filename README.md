# PriceImpactCancellations
Public repository submitted as part MSc thesis. Only one example file for extracting features from LOBSTER data is available. Already extracted features and files regarding the other experiments are available upon request.

## utils
Contains all files used for preprocssing the LOBSTER data. More precisely each files achieves the following:
### utils_load.py
  - Loads LOBSTER message for a particular file into a df
  - Loads LOBSTER orderbook file into a df. Has the option of loading the orderbook file that matches the date of the message file
  
### utils_msg_expand.py
  - Expands message file by adding Mid Price, Best Bid, Best Ask
  - Adds level at which order is located at event trigger plus the size of the level. For cancellation information is displayed for the period just before the cancellation has happened

### utils_cancel_expand.py
  - Loads cancellations from an expanded message df. 
  - Adds information about arrival of each cancellations order
  - Reconstructs queue postions (! only a proxy to the real queue is used)

### utils_cfi_agg.py
  - Calculates Cancellation Flow Imbalance Feature with aggregated levels

### utils_cfi.py
  - Calculates Cancellation Flow Imbalance Feature for each individual level separately

### utils_returns.py
  - Calculates log returns

### utils_counts.py
  - Calculates counts and total volume information 

## Experiment4_main.py
  - Script for main experiment. Extracts cfi, log returns and cancellation and total volume infromation

## Experiment4_main.sh
  - Shell file to run experiment on SLURM
  - use sbatch Experiment4_main.sh to run
  
