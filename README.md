# CI

This is a general purpose CI system written in Python using py4web, for Linux only.
Think of it as an alterantive to Bamboo or Jenkins suitable for 1-1000 workers.

CI is designed to be super simple to setup. In particular there no setup required on
the workers other that they mush have ``dtach`` installed and the the server must be able to
``ssh -A {worker}`` without a password.

## Installation

### Server Configuration

In ``ci/settings.py`` configure:

- The url to be used by the workers to reach your app (``APP_BASE_URL``)
- Email settings
  (``SMTP_SSL``, ``SMTP_SERVER``, ``SMTP_SENDER``, ``SMTP_LOGIN``, ``SMTP_TLS``)
- Optionall a Single Sign on (``OAUTH2GOOGLE_C*``, `OAUTH2GITHUB_*``, etc.)

### Run it (with uv)

    uv run --with-requirements apps/ci/requirements.txt py4web run apps

### Workers configuration










