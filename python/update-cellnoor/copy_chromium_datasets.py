import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

METRICS_SUMMARY_FILENAMES = {
    "cellranger": "metrics_summary.csv",
    "cellranger-arc": "summary.csv",
    "cellranger-atac": "summary.json",
    "cellranger-multi": "metrics_summary.csv",
    "cellranger-multi-hto": "metrics_summary.csv",
    "cellranger-multi-ocm": "metrics_summary.csv",
    "cellranger-multi-vdj": "metrics_summary.csv",
    "cellranger-vdj": "metrics_summary.csv",
}


def _get_cellranger_directory(dataset_directory: Path) -> Path:
    return next(
        subdir for subdir in dataset_directory.iterdir() if "cellranger" in subdir.name
    )


def get_cmdline_file(dataset_directory: Path) -> Path:
    cellranger_directory = _get_cellranger_directory(dataset_directory)
    return cellranger_directory / "_files" / "_cmdline"


def get_pipeline_metadata_file(dataset_directory: Path) -> Path:
    path = dataset_directory / "pipeline-metadata.json"

    return path


@dataclass(frozen=True, kw_only=True)
class CellrangerOutputFiles:
    _metrics: list[Path]
    _qc_library_metrics: Path
    _qc_report: Path
    _qc_sample_metrics: Path
    _web_summaries: list[Path]

    @property
    def files(self) -> list[tuple[str, Path]]:
        files = [
            *self._metrics,
            self._qc_library_metrics,
            self._qc_report,
            self._qc_sample_metrics,
            *self._web_summaries,
        ]

        ret = []

        for f in files:
            if not f.exists():
                continue

            if "cellranger" in f.parent.name:
                filename = f.name
            else:
                sample_dir = f.parent
                per_sample_outs_dir = sample_dir.parent
                filename = f"{per_sample_outs_dir.name}/{sample_dir.name}/{f.name}"

            ret.append((filename, f))

        return ret


def _get_files_from_per_sample_outs(
    dataset_directory: Path,
) -> CellrangerOutputFiles | None:
    cellranger_directory = _get_cellranger_directory(dataset_directory)
    per_sample_outs = cellranger_directory / "per_sample_outs"

    if not per_sample_outs.exists():
        return

    cellranger10_filenames = [
        "qc_library_metrics.csv",
        "qc_report.csv",
        "qc_sample_metrics.csv",
    ]
    _qc_library_metrics, _qc_report, _qc_sample_metrics = [
        cellranger_directory / p for p in cellranger10_filenames
    ]

    return CellrangerOutputFiles(
        _qc_library_metrics=_qc_library_metrics,
        _qc_report=_qc_report,
        _qc_sample_metrics=_qc_sample_metrics,
        _metrics=[
            sample_dir / METRICS_SUMMARY_FILENAMES[cellranger_directory.name]
            for sample_dir in per_sample_outs.iterdir()
        ],
        _web_summaries=[
            sample_dir / "web_summary.html" for sample_dir in per_sample_outs.iterdir()
        ],
    )


def _get_files_from_cellranger_directory(
    dataset_directory: Path,
) -> CellrangerOutputFiles:
    cellranger_directory = _get_cellranger_directory(dataset_directory)

    return CellrangerOutputFiles(
        _qc_library_metrics=cellranger_directory / "qc_library_metrics.csv",
        _qc_report=cellranger_directory / "qc_report.csv",
        _qc_sample_metrics=cellranger_directory / "qc_sample_metrics.csv",
        _metrics=[
            cellranger_directory / METRICS_SUMMARY_FILENAMES[cellranger_directory.name]
        ],
        _web_summaries=[cellranger_directory / "web_summary.html"],
    )


def get_cellranger_output_files(
    dataset_directory: Path,
) -> CellrangerOutputFiles:
    if files := _get_files_from_per_sample_outs(dataset_directory):
        return files

    return _get_files_from_cellranger_directory(dataset_directory)


def _destination_file_path(
    source_dataset_directory: Path, source_file: Path, destination_directory: Path
) -> Path:
    cellranger_directory = _get_cellranger_directory(source_dataset_directory)

    per_sample_outs = cellranger_directory / "per_sample_outs"
    if per_sample_outs.exists():
        return (
            destination_directory
            / "per_sample_outs"
            / source_file.parent.name
            / source_file.name
        )
    else:
        return destination_directory / cellranger_directory.name / source_file.name


def _copy_dataset_directory(source_dataset_directory: Path, destination: Path):
    try:
        source_cellranger_directory = _get_cellranger_directory(
            source_dataset_directory
        )
    except StopIteration:
        return
    except NotADirectoryError:
        return

    destination_directory = destination / source_cellranger_directory.parent.name

    if destination_directory.exists():
        return

    destination_files_directory = (
        destination_directory / source_cellranger_directory.name / "_files"
    )
    destination_files_directory.mkdir(exist_ok=True, parents=True)
    shutil.copyfile(
        get_cmdline_file(source_dataset_directory),
        destination_files_directory / "_cmdline",
    )

    source_pipeline_metadata = get_pipeline_metadata_file(source_dataset_directory)
    shutil.copyfile(
        source_pipeline_metadata, destination_directory / source_pipeline_metadata.name
    )

    fileset = get_cellranger_output_files(source_dataset_directory)
    for source_file in [
        *fileset._metrics,
        fileset._qc_library_metrics,
        fileset._qc_report,
        fileset._qc_sample_metrics,
        *fileset._web_summaries,
    ]:
        if not source_file.exists():
            continue

        destination_file = _destination_file_path(
            source_dataset_directory=source_dataset_directory,
            source_file=source_file,
            destination_directory=destination_directory,
        )

        destination_file.parent.mkdir(exist_ok=True, parents=True)
        shutil.copyfile(source_file, destination_file)


def main():
    top_level_source_directories = Path("/sc/service/delivery").glob("*/*/2*")

    destination = Path(sys.argv[1])

    for dataset_directory in top_level_source_directories:
        _copy_dataset_directory(
            source_dataset_directory=dataset_directory, destination=destination
        )


if __name__ == "__main__":
    main()
