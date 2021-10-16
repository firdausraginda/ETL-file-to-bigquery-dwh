#

## Depedencies

- install pandas
```
pipenv install pandas
```

- install [google auth oauthlib](https://cloud.google.com/docs/authentication/end-user)
```
pipenv install google-auth-oauthlib
```

- install [google cloud bigquery](https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python)
```
pipenv install google-cloud-bigquery
```

- install [pandas gbq](https://cloud.google.com/bigquery/docs/pandas-gbq-migration)
```
pipenv install pandas-gbq 'google-cloud-bigquery[bqstorage,pandas]'
```

## References
- [authenticating with a service account key file](https://cloud.google.com/bigquery/docs/authentication/service-account-file)