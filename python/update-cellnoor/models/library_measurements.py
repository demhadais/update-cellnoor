# try:
#     measurement1 = NucleicAcidMeasurementData.Electrophoretic(
#         measured_at=measured_at,
#         instrument_name="Agilent TapeStation",
#         mean_size_bp=str_to_float(row["tapestation_mean_library_size_(bp)"]),
#         concentration=NucleicAcidConcentration(
#             unit=(MassUnit.Picogram, VolumeUnit.Microliter),
#             value=str_to_float(row["tapestation_concentration_(pg/µl)"]),
#         ),
#         sizing_range=tuple(
#             int(str_to_float(row[k]))
#             for k in (
#                 "tapestation_gate_range_minimum_(bp)",
#                 "tapestation_gate_range_maximum_(bp)",
#             )
#         ),
#     )
# except Exception:
#     measurement1 = None
# try:
#     measurement2 = NucleicAcidMeasurementData.Fluorometric(
#         measured_at=measured_at,
#         instrument_name="ThermoFisher Qubit",
#         concentration=NucleicAcidConcentration(
#             unit=(MassUnit.Nanogram, VolumeUnit.Microliter),
#             value=str_to_float(row["qubit_concentration_(ng/µl)"]),
#         ),
#     )
# except Exception:
#     measurement2 = None

# measurements = [
#     NewLibraryMeasurement(measured_by=preparer_ids[0], data=data)
#     for data in [measurement1, measurement2]
#     if data is not None
# ]
# data["measurements"] = measurements
