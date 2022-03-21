"""
Create sampling frames for manual annotation.
"""
from pathlib import Path

from fos.gcp import write_query


def main():
    # Create sampling frame
    write_query(Path("sampling_frame.sql"),
                destination="field_model_replication.sampling_frame",
                clobber=True)

    # Create EN CS sampling frame / stratum
    write_query(Path("en_cs_frame.sql"),
                destination="field_model_replication.en_cs_sampling_frame",
                clobber=True)

    # Create EN non-CS sampling frame / stratum
    write_query(Path("en_non_cs_frame.sql"),
                destination="field_model_replication.en_non_cs_sampling_frame",
                clobber=True)


if __name__ == '__main__':
    main()
