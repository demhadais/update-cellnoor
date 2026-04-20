# import asyncio
# from collections.abc import Generator, Iterator
# from typing import Any

# import aiohttp

# from utils import (
#     NO_LIMIT_QUERY,
#     date_str_to_eastcoast_9am,
#     property_id_map,
# )


# def _parse_row(row: dict[str, Any]) -> dict[str, Any]:
#     data = {
#         "readable_id": row["readable_id"],
#         "additional_data": {"ilab_request_ids": []},
#     }

#     try:
#         data["begun_at"] = date_str_to_eastcoast_9am(row["sequenced_date"]).isoformat()
#         data["finished_at"] = date_str_to_eastcoast_9am(
#             row["completed_date"]
#         ).isoformat()
#     except AttributeError:
#         pass

#     data["additional_data"]["ilab_request_ids"].append(row["ilab_project_id"])

#     return data


# async def csv_to_new_sequencing_runs(
#     data: list[dict[str, Any]],
# ) -> Generator[dict[str, Any]]:
#     sequencing_runs = (_parse_row(row) for row in data)

#     return (run for run in sequencing_runs if run is not None)
