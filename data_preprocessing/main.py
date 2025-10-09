"""
Preprocessing Strategy - 2 Layer Process
=========================================

Architecture Overview:
---------------------
                    RAW DATA (PostgreSQL)
                            ↓
        ┌───────────────────────────────────────────┐
        │   Layer 1: Retrieval & Standardization    │
        │   - Retrieve data from PostgreSQL         │
        │   - Transform to standardized format      │
        │   - Cache standardized data               │
        └───────────────────────────────────────────┘
                            ↓
                    [CACHE STORAGE]
                    (Faster Retrieval)
                            ↓
        ┌───────────────────────────────────────────┐
        │   Layer 2: Final Transformation           │
        │   - Load from cache or Layer 1            │
        │   - Apply business logic                  │
        │   - Transform to desired schema           │
        └───────────────────────────────────────────┘
                            ↓
                    DESIRED DATAFRAME


Data Flow:
----------
RAW DATA
    ↓
Layer 1: Data Retrieval, Standardization & Caching
    - Functions that retrieve data from PostgreSQL
    - Transform raw data into a general/standardized format
    - Cache standardized data for faster retrieval in future runs
    - Output: Generic formatted data structure
    ↓
    [CACHED for subsequent runs]
    ↓
Layer 2: Final Transformation
    - Consume generic formatted data from Layer 1 (or cache, if available)
    - Transform to desired data structure/schema
    - Apply business logic and final processing
    ↓
DESIRED DATAFRAME
"""


from data_preprocessing.second_layer_fns import *
