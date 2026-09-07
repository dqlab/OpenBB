# Providers

In this folder you can find the providers that were created or are supported by OpenBB.

## Recommended structure

Every provider is located within a directory, with the following structure:

```{.bash}
openbb_platform
└───providers
    └───<provider_name>
        |   README.md
        │   pyproject.toml
        │   poetry.lock
        |───tests
        └───openbb_<provider_name>
            │   __init__.py
            |───models
            |   |───<some model>.py
            |   └───...
            └───utils
                |───<some helper>.py
                └───...
```

The models define the data structures that are used to query the provider endpoints and store the response data.

See [CONTRIBUTING file](../CONTRIBUTING.md) for more details

## Interactive Brokers

The [IBKR provider](ibkr/README.md) includes standard equity quote/historical
fetchers and the `obb.ibkr` account, portfolio, and market-data router.
Install with `python -m pip install -e ./openbb_platform/providers/ibkr` from
the repository root, then run `openbb-build`. A TWS/IB Gateway session is
required for live calls.
