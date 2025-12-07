import requests
import json
from pathlib import Path
import os
from typing import Callable, Iterable, Dict, Any, List

from utils import is_in_stable_beams, has_beam_type, has_bad_detector_quality, has_good_detector_quality


RunFilter = Callable[[Dict[str, Any]], bool]


def fetch_and_save_runs(
    url: str,
    raw_output_path: str = "runs_raw.json",
    stable_output_path: str = "bkkp_data/runs_stable_beams_with_good_tpc_quality.json",
    filters: List[RunFilter] | None = None,
):
    """
    Fetches JSON from the given API URL, optionally saves the raw response,
    applies a list of filter functions, and saves the filtered result.

    `filters` is a list of callables: filter(run) -> bool.
    A run is kept if ALL filters return True.
    """

    print(f"Requesting data from: {url}")

    BASE_DIR = Path(__file__).resolve().parent
    ca_bundle = os.path.join(BASE_DIR, "ali-bookkeeping.cern.ch.pem")

    response = requests.get(url, verify=ca_bundle, timeout=30)
    response.raise_for_status()

    data = response.json()
    runs: Iterable[Dict[str, Any]] = data.get("data", [])
    print(f"Total runs received: {len(runs)}")

    # Optional: save raw data
    # raw_path = Path(raw_output_path)
    # with raw_path.open("w", encoding="utf-8") as f:
    #     json.dump(data, f, indent=2)

    if filters is None: print("No additional filters are applied on the runs.")

    def apply_filters(run: dict) -> bool:
        return all(f(run) for f in filters)

    # Use apply_filters(run) in list comprehension
    runs_with_filters = [run for run in runs if apply_filters(run)]

    print(f"Runs passing filters: {len(runs_with_filters)}")

    stable_path = Path(stable_output_path)
    stable_path.parent.mkdir(parents=True, exist_ok=True)
    with stable_path.open("w", encoding="utf-8") as f:
        json.dump(runs_with_filters, f, indent=2)
    print(f"Filtered runs saved to: {stable_path.resolve()}")

    return runs_with_filters


if __name__ == "__main__":
    API_URL = (
        "https://ali-bookkeeping.cern.ch/api/runs"
        "?page[limit]=5000"
        "&filter[detectors][values]=TPC"
        "&filter[detectors][operator]=and"
        "&filter[tags][values]=Not+for+physics"
        "&filter[tags][operation]=none"
    )

    custom_filters = [
        is_in_stable_beams,
        lambda run: has_beam_type(run, "PP"),
        lambda run: not has_bad_detector_quality(run, "TPC"),
    ]

    fetch_and_save_runs(
        API_URL,
        stable_output_path="bkkp_data/runs_stable_beams_PP_no_bad_TPC.json",
        filters=custom_filters,
    )
