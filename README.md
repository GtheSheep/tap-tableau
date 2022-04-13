# tap-tableau

`tap-tableau` is a Singer tap for Tableau using the [metadata API](https://help.tableau.com/current/api/metadata_api/en-us/index.html)
and the [REST API](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api.htm) via [TableauServerClient](https://tableau.github.io/server-client-python/).

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

```bash
pipx install git+https://github.com/gthesheep/tap-tableau.git
```

## Configuration

Currently, the taps are separated, so you will have to run them both in order to get all the data, though the
datasets are distinct so can coexist, please see `meltano.yml` for an example.  

### Accepted Config Options

`server_url` - Url for your Tableau Server/ Online  
`api_version` - API version to use  
`site_url_id` - Site ID  
`personal_access_token_name` - Name for access token for authentication  
`personal_access_token_secret` - Access token secret for authentication  

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-tableau --about
```

### Source Authentication and Authorization

For authentication using Personal Access Tokens see [this guide](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_auth.htm#make-a-sign-in-request-with-a-personal-access-token).

## Usage

You can easily run `tap-tableau` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-tableau --version
tap-tableau --help
tap-tableau --config CONFIG --discover > ./catalog.json
```

## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_tableau/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-tableau` CLI interface directly using `poetry run`:

```bash
poetry run tap-tableau --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-tableau
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-tableau --version
# OR run a test `elt` pipeline:
meltano elt tap-tableau target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to 
develop your own taps and targets.
