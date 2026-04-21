# def _parse_group(
#     sequencing_run_readable_id: str,
#     libraries: dict[str, str],
#     rows: Iterator[dict[str, Any]],
# ) -> dict[str, Any] | None:
#     sequencing_submissions = []
#     data: dict[str, Any] = {
#         "readable_id": sequencing_run_readable_id,
#         "additional_data": {"ilab_request_ids": []},
#     }

#     time_keys = [("sequenced_date", "begun_at"), ("completed_date", "finished_at")]

#     for row in rows:
#         for row_key, data_key in time_keys:
#             if row[row_key] is None:
#                 return None
#             datetime = date_str_to_eastcoast_9am(row[row_key])
#             if data.get(data_key) is None:
#                 data[data_key] = datetime
#             elif data[data_key] != datetime:
#                 raise ValueError(
#                     f"rows in the same group do not share the same `{datetime}`: {sequencing_run_readable_id}"
#                 )

#         data["additional_data"]["ilab_request_ids"].append(row["ilab_project_id"])

#         sequencing_submission = {}
#         sequencing_submission["library_id"] = library_id = libraries.get(row["sc_id"])

#         if library_id is None:
#             continue

#         sequencing_submission["submitted_at"] = shitty_date_str_to_eastcoast_9am(
#             row["date_arrived"]
#         )

#         sequencing_submissions.append(NewSequencingSubmission(**sequencing_submission))

#     return NewSequencingRun(**data, libraries=sequencing_submissions)


# def _group_rows(data: list[dict[str, Any]]) -> groupby:
#     def sorting_and_grouping_key(d: dict[str, str]):
#         return (str(d["sequenced_date"]), str(d["completed_date"]))

#     sorted_data = sorted(data, key=sorting_and_grouping_key)
#     filtered_data = filter(lambda d: d["ilab_project_id"] is not None, sorted_data)
#     grouped_data = groupby(filtered_data, key=sorting_and_grouping_key)

#     return grouped_data


# import asyncio
# from collections.abc import Generator, Iterator
# from itertools import groupby
# from typing import Any

# from utils import (
#     date_str_to_eastcoast_9am,
#     property_id_map,
# )


# def _parse_group(
#     sequencing_run_readable_id: str,
#     libraries: dict[str, str],
#     rows: Iterator[dict[str, Any]],
# ) -> dict[str, Any] | None:
#     sequencing_submissions = []
#     data: dict[str, Any] = {
#         "readable_id": sequencing_run_readable_id,
#         "additional_data": {"ilab_request_ids": []},
#     }

#     time_keys = [("sequenced_date", "begun_at"), ("completed_date", "finished_at")]

#     for row in rows:
#         for row_key, data_key in time_keys:
#             if row[row_key] is None:
#                 return None
#             datetime = date_str_to_eastcoast_9am(row[row_key])
#             if data.get(data_key) is None:
#                 data[data_key] = datetime
#             elif data[data_key] != datetime:
#                 raise ValueError(
#                     f"rows in the same group do not share the same `{datetime}`: {sequencing_run_readable_id}"
#                 )

#         data["additional_data"]["ilab_request_ids"].append(row["ilab_project_id"])

#         sequencing_submission = {}
#         sequencing_submission["library_id"] = library_id = libraries.get(row["sc_id"])

#         if library_id is None:
#             continue

#         sequencing_submission["submitted_at"] = shitty_date_str_to_eastcoast_9am(
#             row["date_arrived"]
#         )

#         sequencing_submissions.append(NewSequencingSubmission(**sequencing_submission))

#     return NewSequencingRun(**data, libraries=sequencing_submissions)


# def _yield_sequencing_runs(
#     last_run_readable_id: str, libraries: dict[str, UUID], grouped_rows: groupby
# ) -> Generator[NewSequencingRun | None]:
#     for (_, _), rows in grouped_rows:
#         last_run_readable_id = str(int(last_run_readable_id) + 1)
#         yield _parse_group(last_run_readable_id, libraries, rows)


# async def csv_to_sequencing_runs(
#     client: ScamplersClient, data: list[dict[str, Any]]
# ) -> Generator[NewSequencingRun]:
#     async with asyncio.TaskGroup() as tg:
#         libraries = tg.create_task(client.list_libraries(LibraryQuery(limit=99_999)))
#         pre_existing_sequencing_runs = tg.create_task(
#             client.list_sequencing_runs(SequencingRunQuery(limit=99_999))
#         )

#     libraries, pre_existing_sequencing_runs = (
#         libraries.result(),
#         pre_existing_sequencing_runs.result(),
#     )
#     libraries = property_id_map("info.summary.readable_id", "info.id_", libraries)
#     pre_existing_sequencing_runs = property_id_map(
#         "summary.readable_id", "summary.id", pre_existing_sequencing_runs
#     )
#     last_run_id = sorted(
#         pre_existing_sequencing_runs, key=lambda readable_id: int(readable_id)
#     )
#     try:
#         last_run_id = last_run_id[-1]
#     except IndexError:
#         last_run_id = "0"

#     grouped_rows = _group_rows(data)

#     sequencing_runs = _yield_sequencing_runs(last_run_id, libraries, grouped_rows)
#     return (
#         run
#         for run in sequencing_runs
#         if not (run is None or run.readable_id in pre_existing_sequencing_runs)
#     )
