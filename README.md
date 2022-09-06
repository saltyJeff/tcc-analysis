# tcc-analysis

The purpose of this repository is perform an analysis of rents, based on scraped data from Craigslist.

For more information, see this repository: https://github.com/jasonkarpman/TCC_Evaluation

## Architecture

1. Every day, the [scraper script](https://github.com/jasonkarpman/TCC_Evaluation/blob/master/housing/programs/craigslist_scraper_aws.py) is executed on a fleet of EC2 instances that will scrape Craigslist data and upload it to a MySQL database
2. On demand, a user can enter a sagemaker notebook that contains the files in this repository, and execute one of two scripts:
  - `export_db.ipynb` will query all the data from the MySQL database, filter and clean it up, and perform a spatial join with the relevant census tracts. The data will be uploaded into a S3 bucket for other programs to query from
  - `did.ipynb` will take data exported above from S3 and perform a differences in difference model.

## File Overview

- `tracts/*.geojson` defines the different census tracts of interest
- `did.ipynb` performs a differences in differences test
- `export_db.ipynb` exports the MySQL database to S3 as a parquet file and excel file, joined on the census tracts

## Setup

Due to the size of the data, the scripts use Spark and are meant to be executed within a Sagemaker Notebook. To instance a new notebook:

1. Create a new Sagemaker Notebook instance. Settings are:
  - Set the new instance's security group to the same one as the database. As of this writing it is `sg-0c8866efc64ef0416`
  - Set instance type to whatever you want, I find that ml.t3.xlarge works well
2. Enter the sagemaker notebook and create a terminal
3. Due to a bug that AWS as of this writing has yet to fix (https://github.com/aws/sagemaker-spark/issues/84#issuecomment-1186432966), a new conda environment will need to be created:
  1. Open a new terminal, and enter `bash` (the default terminal is plain `sh`)
  2. run `setup.sh`, this will install the new environment
  3. go to the `*.ipynb`'s and change the kernel to spark_2_4_0

**If you want to stop the instance to save money and restart it later, then you will have to go through steps 2 and below again**

## Future Improvements

As with all scraping projects, there is a large amount of filtering that must be done. [Here is a wishlist of all the filtering steps](https://docs.google.com/document/d/1BqNj1Co4mm4H_ilM16SrbUDsdtj8djInfuIYDtnhde4/edit)

I've summarized a path forward for some of them below

### Dataframe setup

> Add columns for the following:
> 
> - Unique_ID
> - Duplicate_ID
> - Exclusion_type
> - Duplicate_type
>   Assign a new unique ID to each posting in our data set separate from its Posting ID and it under “Unique_ID” column

The unique ID will be relatively simple to include, as we can just use the row number as a unique ID

### Exclusion Rules

> Exclude postings with the following keywords in the post title:
> "commercial"
> 
> - Flag these listings by adding a 1 to the "Exclusion_type" column 
>   "office"
>   
> - Flag these listings by adding a 2 to the "Exclusion_type" column
>   "offices"
>   
> - Flag these listings by adding a 3 to the "Exclusion_type" column
>   "business"
>   
> - Flag these listings by adding a 4 to the "Exclusion_type" column
>   "businesses"
>   
> - Flag these listings by adding a 5 to the "Exclusion_type" column
>   "art"
>   
> - Flag these listings by adding a 6 to the "Exclusion_type" column
>   "artist"
>   
> - Flag these listings by adding a 7 to the "Exclusion_type" column
>   "artists"
>   
> - Flag these listings by adding a 8 to the "Exclusion_type" column
>   Duplicate Removal Rules
>   

This can be accomplished with a regex column in spark. We probably don't even need to alias each exclusion type to a number.

> - If any of the variables are them same then flag them as duplicates
>   Square footage / price / POST ID 
>   Flag these listings by adding a 1 to the "Duplicate_type" column
>   Square footage / price / REPOST ID 
>   Flag these listings by adding a 2 to the "Duplicate_type" column
>   Square feet / price / bedrooms
>   Flag these listings by adding a 3 to the "Duplicate_type" column
>   Assign a new ID to each instance of a duplicate such that the ID Is the same for a set of duplicates but no other postings, add add the ID under “Duplicate_ID” column 
>   Keep the first instance of the duplicate and remove/hide all other instances
>   

These are more expensive queries, since they will require a shuffling of the data, but can still be accomplished using regex

> - Queries
>   What % of scraped apartment posts lack pricing?
>   What % of scraped apartment posts lack square footage?
>   What % of scraped apartment posts lack X/Y coordinates?
>   What % of scraped apartment posts fall within each of the exclusion types?
>   What % of scraped apartment posts fall within each of the duplicate types?
>   What % of scraped apartment posts made it into our final dataframe?
>   

These queries are statistics on the quality of our scraped data. They should be added to the `export_db.ipynb` script

> - How many posts (n) fall within each of the following groups:
>   Fresno TCC Site - Year 1
>   Fresno TCC Site - Year 3
>   Fresno Control Sites - Year 1
>   Fresno Control Sites - Year 3
>   Ontario TCC Site - Year 1
>   Ontario TCC Site - Year 3
>   Ontario Control Sites - Year 1
>   Ontario Control Sites - Year 3
>   Watts TCC Site - Year 1
>   Watts TCC Site - Year 3
>   Watts Control Sites - Year 1
>   Watts Control Sites - Year 3
>   Pacoima TCC Site - Year 1
>   Pacoima TCC Site - Year 3
>   Pacoima Control Sites - Year 1
>   Pacoima Control Sites - Year 3
>   

Bits of these queries are already in `did.ipynb`, but not plotted out.

> - DiD Analysis
>   Can we run a DiD model without binning data at the census tract level first? 
>   Can we run a separate DiD model for each TCC region?
>   Fresno
>   Ontario
>   Watts
>   Pacoima
>   

I've added this to `did.ipynb`. CTRL+F for `GROUP_BY_REGION` and set it to true, then the DID will be binned per-region instead of per-tract.

*I also have concerns about the DID, since it operates using medians instead of averages*