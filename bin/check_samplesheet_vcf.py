#!/usr/bin/env python


"""Provide a command line tool to validate and transform tabular samplesheets."""


import argparse
import csv
import logging
import sys
from collections import Counter
from pathlib import Path

logger = logging.getLogger()


class RowChecker:
    """
    Define a service that can validate and transform each given row.

    Attributes:
        modified (list): A list of dicts, where each dict corresponds to a previously
            validated and transformed row. The order of rows is maintained.

    """

    VALID_FORMATS = (
        ".vcf.gz",
        ".json",
    )

    def __init__(self, sample="sample", vcf="vcf", traits="traits", ancestry="ancestry", **kwargs):
        """
        Initialize the row checker with the expected column names.

        Args:
            sample (str): The name of the column that contains the sample name
                (default "sample").
            vcf_ (str): The name of the column that contains the VCF file path
                (default "vcf").
            traits (str): The name of the column that contains the traits JSON file path
                (default "traits").
            ancestry (str): The name of the column that contains the ancestry JSON file path
                (default "ancestry").
        """
        self._sample = sample
        self._vcf = vcf
        self._traits = traits
        self._ancestry = ancestry
        self._seen = set()
        self.modified = []


    def validate_and_transform(self, row):
        """
        Perform all validations on the given row.
        Args:
            row (dict): A mapping from column headers (keys) to elements of that row
                (values).
        """
        self._validate_sample(row)
        self._seen.add(row[self._sample])
        self.modified.append(row)


    def _validate_sample(self, row):
        """Assert that the VCF, traits and ancestry file has one of the expected extensions."""
        #vcf_file = row[self._vcf]
        #if not any(vcf_file.endswith(extension) for extension in self.VALID_FORMATS):
        #    raise AssertionError(
        #        f"The VCF file has an unrecognized extension: {vcf_file}\n"
        #        f"It should be one of: {', '.join(self.VALID_FORMATS)}"
        #    )
        if not row[self._vcf].endswith(".vcf.gz"):
            raise AssertionError(f"Unexpected VCF file extension: {row[self._vcf]}. The valid VCF extension should be: vcf.gz.")
        if not row[self._traits].endswith("_traits-json.json"):
            raise AssertionError(f"Unexpected traits file extension: {row[self._traits]}. The valid VCF extension should be: _traits-json.json.")
        if not row[self._ancestry].endswith("_ancestry-json.json"):
            raise AssertionError(f"Unexpected ancestry file extension: {row[self._ancestry]}. The valid VCF extension should be: _ancestry-json.json.")



    def validate_unique_samples(self):
        """
        Assert that the combination of sample name and VCF filename is unique.

        In addition to the validation, also rename all samples to have a suffix of _T{n}, where n is the
        number of times the same sample exist, but with different VCF files, e.g., multiple runs per experiment.

        """
        if len(self._seen) != len(self.modified):
            raise AssertionError("The pair of sample name and VCF must be unique.")
        seen = Counter()
        for row in self.modified:
            sample = row[self._sample]
            seen[sample] += 1
            row[self._sample] = f"{sample}_T{seen[sample]}"


def read_head(handle, num_lines=10):
    """Read the specified number of lines from the current position in the file."""
    lines = []
    for idx, line in enumerate(handle):
        if idx == num_lines:
            break
        lines.append(line)
    return "".join(lines)


def sniff_format(handle):
    """
    Detect the tabular format.

    Args:
        handle (text file): A handle to a `text file`_ object. The read position is
        expected to be at the beginning (index 0).

    Returns:
        csv.Dialect: The detected tabular format.

    .. _text file:
        https://docs.python.org/3/glossary.html#term-text-file

    """
    peek = read_head(handle)
    handle.seek(0)
    sniffer = csv.Sniffer()
    if not sniffer.has_header(peek):
        logger.critical("The given sample sheet does not appear to contain a header.")
        sys.exit(1)
    dialect = sniffer.sniff(peek)
    return dialect


def check_samplesheet(file_in, file_out):
    """
    Check that the tabular samplesheet has the structure expected by nf-core pipelines.

    Validate the general shape of the table, expected columns, and each row.

    Args:
        file_in (pathlib.Path): The given tabular samplesheet. The format can be either
            CSV, TSV, or any other format automatically recognized by ``csv.Sniffer``.
        file_out (pathlib.Path): Where the validated and transformed samplesheet should
            be created; always in CSV format.

    Example:
        This function checks that the samplesheet follows the following structure,
        see also the `viral recon samplesheet`_::

            sample,vcf,json
            SAMPLE_PE,SAMPLE_PE_RUN1_1.vcf.gz,SAMPLE_PE_RUN1_2.json
            SAMPLE_PE,SAMPLE_PE_RUN2_1.vcf.gz,SAMPLE_PE_RUN2_2.json
            SAMPLE_SE,SAMPLE_SE_RUN1_1.vcf.gz,

    .. _viral recon samplesheet:
        https://raw.githubusercontent.com/nf-core/test-datasets/viralrecon/samplesheet/samplesheet_test_illumina_amplicon.csv

    """
    required_columns = {"sample", "vcf", "traits", "ancestry"}
    # See https://docs.python.org/3.9/library/csv.html#id3 to read up on `newline=""`.
    with file_in.open(newline="") as in_handle:
        reader = csv.DictReader(in_handle, dialect=sniff_format(in_handle))
        # Validate the existence of the expected header columns.
        if not required_columns.issubset(reader.fieldnames):
            req_cols = ", ".join(required_columns)
            logger.critical(f"The sample sheet **must** contain these column headers: {req_cols}. It currently contains {reader.fieldnames}")
            sys.exit(1)
        # Validate each row.
        checker = RowChecker()
        for i, row in enumerate(reader):
            try:
                checker.validate_and_transform(row)
            except AssertionError as error:
                logger.critical(f"{str(error)} On line {i + 2}.")
                sys.exit(1)
        checker.validate_unique_samples()
    header = list(reader.fieldnames)
    header.insert(1, "single_end")
    # See https://docs.python.org/3.9/library/csv.html#id3 to read up on `newline=""`.
    with file_out.open(mode="w", newline="") as out_handle:
        writer = csv.DictWriter(out_handle, header, delimiter=",")
        writer.writeheader()
        for row in checker.modified:
            writer.writerow(row)


def parse_args(argv=None):
    """Define and immediately parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Validate and transform a tabular samplesheet.",
        epilog="Example: python check_samplesheet.py samplesheet.csv samplesheet.valid.csv",
    )
    parser.add_argument(
        "file_in",
        metavar="FILE_IN",
        type=Path,
        help="Tabular input samplesheet in CSV or TSV format.",
    )
    parser.add_argument(
        "file_out",
        metavar="FILE_OUT",
        type=Path,
        help="Transformed output samplesheet in CSV format.",
    )
    parser.add_argument(
        "-l",
        "--log-level",
        help="The desired log level (default WARNING).",
        choices=("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"),
        default="WARNING",
    )
    return parser.parse_args(argv)


def main(argv=None):
    """Coordinate argument parsing and program execution."""
    args = parse_args(argv)
    logging.basicConfig(level=args.log_level, format="[%(levelname)s] %(message)s")
    if not args.file_in.is_file():
        logger.error(f"The given input file {args.file_in} was not found!")
        sys.exit(2)
    args.file_out.parent.mkdir(parents=True, exist_ok=True)
    check_samplesheet(args.file_in, args.file_out)


if __name__ == "__main__":
    sys.exit(main())
