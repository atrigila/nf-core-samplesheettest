//
// Check input samplesheet and get vcf and json channels
//

include { SAMPLESHEET_CHECK_VCF } from '../../modules/local/samplesheet_check_vcf'

workflow INPUT_CHECK_VCF {
    take:
    samplesheet // file: /path/to/samplesheet.csv

    main:
    SAMPLESHEET_CHECK_VCF ( samplesheet )
        .csv
        .splitCsv ( header:true, sep:',' )
        .map { create_vcf_json_channel(it) }
        .set { vcf_json }

    emit:
    vcf_json                                 // channel: [ val(meta), vcf, json ]
    versions = SAMPLESHEET_CHECK_VCF.out.versions // channel: [ versions.yml ]
}

// Function to get list of [ meta, vcf, json ]
def create_vcf_json_channel(LinkedHashMap row) {
    // create meta map
    def meta = [:]
    meta.id = row.sample

    // add path(s) of the vcf and json file(s) to the meta map
    def vcf_json_meta = []
    if (!file(row.vcf).exists()) {
        exit 1, "ERROR: Please check input samplesheet -> VCF file does not exist!\n${row.vcf}"
    }
    if (!file(row.json).exists()) {
        exit 1, "ERROR: Please check input samplesheet -> JSON file does not exist!\n${row.json}"
    }
    vcf_json_meta = [ meta, file(row.vcf), file(row.json) ]
    return vcf_json_meta
}
