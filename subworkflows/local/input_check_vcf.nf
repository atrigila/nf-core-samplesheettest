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
    vcf_json                                 // channel: [ val(meta), vcf, ancestry, traits ]
    versions = SAMPLESHEET_CHECK_VCF.out.versions // channel: [ versions.yml ]

}

// Function to get list of [ meta, vcf, ancestry, traits ]
def create_vcf_json_channel(LinkedHashMap row) {
    // create meta map
    def meta = [:]
    meta.id = row.sample

    // add path(s) of the vcf and json file(s) to the meta map
    def vcf_json_meta = []
    if (!file(row.vcf).exists()) {
        exit 1, "ERROR: Please check input samplesheet -> VCF file does not exist!\n${row.vcf}"
    }
    if (!file(row.ancestry).exists()) {
        exit 1, "ERROR: Please check input samplesheet -> ancestry file does not exist!\n${row.ancestry}"
    }
    if (!file(row.traits).exists()) {
        exit 1, "ERROR: Please check input samplesheet -> traits file does not exist!\n${row.traits}"
    }
    vcf_json_meta = [ meta, file(row.vcf), file(row.ancestry), file(row.traits) ]
    return vcf_json_meta
}

