# Step-by-Step Description of `generate_catalogue.py`

1. **Initialize Environment**: Import necessary libraries and set up the environment.
2. **Database Setup**: Create or overwrite the CMESRCV3_DB database if it already exists.
3. **Table Creation**: Define and create tables for HARPS, CMES, FLARES, RAW_HARPS_BBOX, FINAL_CME_HARP_ASSOCIATIONS, OVERLAPS, OVERLAP_RECORDS, PROCESSED_HARPS_BBOX, CMES_HARPS_EVENTS, CMES_HARPS_SPATIALLY_CONSIST, DIMMINGS, NOAA_HARPNUM_MAPPING, NOAAS, and HARPS_KEYWORDS.
4. **Data Loading**: Load data from SWAN filepaths into the RAW_HARPS_BBOX and HARPS_KEYWORDS tables.
5. **Area and Overlap Calculation**: Calculate the area of each HARP region and determine overlaps between HARP regions.
6. **Data Processing**: Process the RAW_HARPS_BBOX data to create the PROCESSED_HARPS_BBOX table, excluding large HARP regions and trimming bounding boxes that extend beyond the limb.
7. **Association Scripts Execution**: Run scripts to associate dimmings, flares, and CMEs with HARP regions.
8. **Data Insertion**: Insert data from LASCO CME catalogue, spatiotemporally consistent CME-HARP associations, dimmings, and flares into the database.
9. **Timestamp Calculation**: Calculate the closest timestamps for dimmings, flares, and CMEs based on the PROCESSED_HARPS_BBOX timestamps.
10. **Event Association**: Associate dimmings, flares, and CMEs with HARP regions based on temporal proximity and other criteria.
11. **Final Associations**: Determine the final CME-HARP associations based on verification levels and insert them into the FINAL_CME_HARP_ASSOCIATIONS table.
