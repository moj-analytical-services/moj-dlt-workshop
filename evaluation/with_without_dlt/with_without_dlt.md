Here are 2 examples of extracting data from APIs, with and without `dlt`.

## Justice data API extraction
The repo comparing the extraction of data from the [Justice data API](https://data.justice.gov.uk/api) with and without `dlt` can be found [here](https://github.com/moj-analytical-services/justice-api-dlt/tree/main). The aim is to collate the summary statistics returned by the Justice API into a single table and load the data to Athena.

The Justice data API is unconventional. It returns an initial response which contains API endpoints within nested child tables. You then need to request each of these endpoints to return the aggregated statistics. The `requests` library is used to iterate over each of these endpoints and append the responses in a list of dicts.

`dlt` is then used to extract the summary data from the response and automatically parses it into 2 separate tables and provides detailed metadata. The `dlt` pipeline destination is set to Athena which loads the extracted data directly to Athena tables.

The advantages of using `dlt` here are:
- Automatically parsing out and inferring the schema of the data extracted from the Justice data API. Without `dlt`, a separate function would need to be written to do this (as seen in the repo).
- Automatic schema evolution.
   - The API response is complex and returns information about many different statistical publications. The schema for the final table is expected to regularly change.
- Easily swapping out destinations.
- Provides detailed metadata after every pipeline run.

Without using `dlt`, bespoke code would have to be written to handle the above points. Additionally, there is a wider workstream in MoJ to move towards a generic approach to extracting data from an API. `dlt` is a likely option for implementing this.

## GitHub GraphQL API extraction for Tech Radar
The MoJ Tech Radar uses the GitHub GraphQL API to extract, create and delete discussions from the [Tech Radar GitHub](https://github.com/moj-analytical-services/data-and-analytics-engineering-tech-radar) repo. We wanted to see if `dlt` could add any improvements to the process of extracting discussions from GitHub as a JSON file. [This PR](https://github.com/moj-analytical-services/data-and-analytics-engineering-tech-radar/pull/368) outlines an approach.

The `dlt init` command generates a lot of helpful example code but no specific solution for extracting GitHub discussions so we needed to write our own query and a `get_discussions` function. We then had to make minor updates to the `dlt.source` and were then able to extract GitHub discussions as a JSON file.

The Tech Radar requires data extracted from GitHub discussions to be used in subsequent calls to the GitHub API to create or delete discussions. This is quite a specific task; the primary purpose of dlt is to easily load data into well-structured datasets, which limits its usefulness for more complex GitHub API queries. Additionally, since the Tech Radar is updated annually, an automated pipeline for regular extractions and incremental loading is not necessary.

Some elements of `dlt` were still helpful:
- The helper functions handled the pagination of API responses.
- If we wanted to load the extracted discussions data to various destinations then `dlt` would be able to easily handle this.
