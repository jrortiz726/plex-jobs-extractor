"""Data modeling utilities for Plex RAW ingestion."""

__all__ = ["apply_all", "apply_plex_data_model"]


def apply_all(*args, **kwargs):
    from agents.data_modeling.plex_data_models import apply_all as _apply_all

    return _apply_all(*args, **kwargs)


def apply_plex_data_model(*args, **kwargs):
    from agents.data_modeling.plex_data_models import (
        apply_plex_data_model as _apply_plex_data_model,
    )

    return _apply_plex_data_model(*args, **kwargs)
