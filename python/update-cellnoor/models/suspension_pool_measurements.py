# # Prepare the necessary data to construct a concentration
# data["measurements"] = []
# suspension_content = child_suspensions[0]["content"]

# readable_id = data["readable_id"]

# # If no GEMs pool was found, it just means it hasn't been run
# try:
#     gems = pool_to_gems[readable_id]
#     chip_run_on = date_str_to_eastcoast_9am(gems["date_chip_run"])
# except KeyError:
#     chip_run_on = pooled_at
#     pass

# measured_by = preparer_ids[0]

# concentrations = [
#     ("pre-storage_cell/nucleus_concentration_(cell-nucleus/ml)", pooled_at),
#     ("cell/nucleus_concentration_(cell-nucleus/ml)", chip_run_on),
# ]
# for key, measured_at in concentrations:
#     if measurement_data := _parse_concentration(
#         row, key, biological_material=suspension_content, measured_at=measured_at
#     ):
#         data["measurements"].append(
#             NewSuspensionPoolMeasurement(
#                 measured_by=measured_by, data=measurement_data
#             )
#         )

# volumes = [
#     ("pre-storage_volume_(µl)", pooled_at),
#     ("volume_(µl)", chip_run_on),
# ]
# for key, measured_at in volumes:
#     if measurement_data := _parse_volume(
#         row, value_key=key, measured_at=measured_at
#     ):
#         data["measurements"].append(
#             NewSuspensionPoolMeasurement(
#                 measured_by=measured_by, data=measurement_data
#             )
#         )

# # Viability is only measured after storage (or if there was no storage at all)
# if measurement_data := _parse_viability(
#     row, value_key="cell_viability_(%)", measured_at=chip_run_on
# ):
#     data["measurements"].append(
#         NewSuspensionPoolMeasurement(measured_by=measured_by, data=measurement_data)
#     )

# # Same with cell/nucleus diameter
# if measurement_data := _parse_cell_or_nucleus_diameter(
#     row,
#     value_key="average_cell/nucleus_diameter_(µm)",
#     biological_material=suspension_content,
#     measured_at=chip_run_on,
# ):
#     data["measurements"].append(
#         NewSuspensionPoolMeasurement(measured_by=measured_by, data=measurement_data)
#     )
