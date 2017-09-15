These scripts and data files are designed to scrape DC Department of Health restaurant inspection report data from https://dc.healthinspections.us/webadmin/dhd_431/web/index.cfm

Here is the workflow:

1) Run scrape_inspection_links.py to generate or update the scraped_inspection_links.csv file. As this downloads the complete set of active links from the page above, processing this script can take a little while.
2) Run scrape_inspection_data.py to process those links in the scraped_inspection_links.csv file that have not already had their data extracted. This will generate a local cache of html files for each link, and either create or append the data to the inspection_summary_data.csv and violation_details_data.csv files.