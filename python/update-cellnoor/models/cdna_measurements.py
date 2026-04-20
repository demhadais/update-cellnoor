# data["measurements"].append(
#     NewCdnaMeasurement(
#         measured_by=preparer_ids[0],
#         data=NucleicAcidMeasurementData.Electrophoretic(
#             measured_at=prepared_at,
#             instrument_name="TapeStation",
#             sizing_range=tuple(
#                 int(row[key])
#                 for key in [
#                     "tapestation_gate_range_minimum_(bp)",
#                     "tapestation_gate_range_maximum_(bp)",
#                 ]
#             ),  # pyright: ignore[reportArgumentType]
#             concentration=NucleicAcidConcentration(
#                 value=str_to_float(row["tapestation_concentration_(pg/µl)"]),
#                 unit=(MassUnit.Picogram, VolumeUnit.Microliter),
#             ),
#             mean_size_bp=None,
#         ),
#     )
