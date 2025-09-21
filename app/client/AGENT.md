# Project Description

We are building a React + Python web app that allows users to input a (list of) biorxiv DOIs and extract growth rate data from all papers that actually contain growth rate data.

Most of the processing will be done by the web server which is written in Python.

## Frontend

Users should be able to:

- Upload or input a list of biorxiv DOIs or links
- Click on "Process" and have a list of papers returned that actually contained growth rate data.
- Papers with growth rate should be shown in a list and highlighted in green with a checkmark if they contain GR data and red if not (with thext saying: "Does not contain growth rate data")

## Backend

Endpoint:

- POST /api/filter:
  - Body: {dois: list[str]}
  - returns true if the paper contains growth rate data and false if not as JSON
