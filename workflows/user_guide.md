# MNC Event Report Workflow -- Setup Guide

This guide explains how to install and configure the **MNC Event
Report** custom workflow in the EcoScope Desktop App.

------------------------------------------------------------------------

## 1. Clone the Repository

Clone the project from GitHub:

    git clone git@github.com:wildlife-dynamics/mnc.git

------------------------------------------------------------------------

## 2. Locate the Workflow Folder

After cloning:

1.  Open the cloned **mnc** repository\
2.  Navigate to:

```{=html}
<!-- -->
```
    workflows/mara_north_event_report

------------------------------------------------------------------------

## 3. Add Workflow to EcoScope Desktop App

1.  Copy the **full folder path** of:

```{=html}
<!-- -->
```
    workflows/mara_north_event_report

2.  Open the **EcoScope Desktop App**\
3.  Go to **Custom Workflows**\
4.  Click **Add Workflow**\
5.  Paste the copied folder path

The workflow template will now appear in the app.

------------------------------------------------------------------------

## 4. Create the Custom Workflow

Once added:

1.  Click on the new workflow template\
2.  Define the workflow using the details below

------------------------------------------------------------------------

## 5. Workflow Configuration

Use the following structure when setting up:

``` yaml
workflow_details:
  description: sample mnc test workflow
  name: sample_mnc_workflow

er_client_name:

data_source:
  name: mnc

time_range:
  since: '2025-12-31T21:00:00.000Z'
  until: '2026-02-01T09:00:00.000Z'
```

------------------------------------------------------------------------

## 6. Information Required From the User

  -----------------------------------------------------------------------
  Field                  Description
  ---------------------- ------------------------------------------------
  **er_client_name**     Name of the client for whom the report is being
                         generated

  **Time Range (since /  The reporting period in UTC format
  until)**               

  **Workflow Name        Custom name if different from default
  (optional)**           
  -----------------------------------------------------------------------

------------------------------------------------------------------------