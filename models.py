"""
py4ci doesn't define its tables here — they're created inside
``CI.define_tables()`` (see ``ci.py``) so the schema lives next to the code
that operates on it. ``db`` itself is set up in ``common.py``.

This file is kept for compatibility with py4web's _dashboard, which expects
every app to have a ``models.py``.
"""
