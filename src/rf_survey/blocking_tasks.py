import logging

from rf_shared.checksum import get_checksum
from rf_shared.models import MetadataRecord
from rf_survey.models import ProcessingJob, ApplicationInfo

logger = logging.getLogger(__name__)


def process_capture_job_blocking(
    job: ProcessingJob, serial: str, app_info: ApplicationInfo
) -> MetadataRecord:
    """
    Performs blocking I/O (saving file), checksumming, and returns a MetadataRecord.
    """
    raw_capture = job.raw_capture
    receiver_config = job.receiver_config_snapshot
    sweep_config = job.sweep_config_snapshot

    timestamp_str = raw_capture.capture_timestamp.strftime("D%Y%m%dT%H%M%SM%f")

    filename = f"{serial}-{app_info.hostname}-{timestamp_str}.sc16"
    file_path = app_info.output_path / filename

    try:
        with open(file_path, "wb") as f:
            f.write(raw_capture.iq_data_bytes)
        logger.debug(f"File stored as {file_path}")
    except IOError as e:
        logger.error(f"Failed to write capture file to disk: {e}", exc_info=True)
        raise

    file_checksum = get_checksum(raw_capture.iq_data_bytes)

    metadata_record = MetadataRecord(
        # Static application info
        hostname=app_info.hostname,
        organization=app_info.organization,
        gcs=app_info.coordinates,
        group=app_info.group,
        serial=serial,
        bit_depth=16,
        # Configuration context from the snapshots
        interval=sweep_config.interval_sec,
        length=receiver_config.duration_sec,
        gain=receiver_config.gain_db,
        sampling_rate=receiver_config.bandwidth_hz,
        # Direct data from the capture itself
        frequency=raw_capture.center_freq_hz,
        timestamp=raw_capture.capture_timestamp,
        # Data generated during this processing step
        source_path=file_path,
        checksum=file_checksum,
    )

    return metadata_record
