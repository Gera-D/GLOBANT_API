# GLOBANT_API

This repository contains my resolution to the Globant's Data Engineering Coding Challenge.
The API is available as a Cloud Run service here: [text](https://globant-api-g5kkg6lgsa-uc.a.run.app)
Please, keep in mind that the route `/insert_records` is only available for 1 use, since it removes the files from the source bucket to prevent unnecesary reprocessings.

## Local execution

For the code to work, please set your ADC following the instructions in [text](https://cloud.google.com/docs/authentication/provide-credentials-adc#local-dev).

If you don't yet have gcloud CLI installed, you can do so from here [text](https://cloud.google.com/sdk/docs/install) prior to following the instructions above.

Note that the following roles are needed and a service account is preferred:
    - Storage Object Admin
    - Service Usage Consumer
    - BigQuery Job User
    - BigQuery Data Editor
    - Viewer

## Endpoints

`/insert_records` Inserts all csv files stored in the bucket at the moment to their corresponding BigQuery tables. Next, moves the files to a new prefix in format YYYY/mm/dd/HH:mm.

`/get_hires_per_quarter_2021` Returns the number of employees hired for each job and department in 2021 divided by quarter ordered alphabetically by department and job.

`/get_hires_greater_than_mean_2021` Returns a list of ids, name and number of employees hired of each department that hired more employees than the mean of employees hired in 2021 for all the departments, ordered by the number of employees hired (descending).