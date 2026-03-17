"""
_GetReprocessingByStatus_

Oracle implementation of GetReprocessingByStatus

Return all reprocessing requests with a given status.
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetReprocessingByStatus(DBFormatter):

    def execute(self, status, conn = None, transaction = False):

        sql = """SELECT id, input_dataset, run_id, min_lumi, max_lumi,
                        global_tag, cmssw_version, scram_arch, proc_version, scenario,
                        reco_split, multicore,
                        write_reco, write_aod, write_miniaod, write_nanoaod, write_dqm,
                        alca_skim, physics_skim, dqm_seq, nano_flavour,
                        status, rucio_rule_id, workflow_name, fileset_id,
                        error_message, created_at, updated_at
                 FROM reprocessing_request
                 WHERE status = :STATUS
                 ORDER BY created_at ASC
                 """

        binds = { 'STATUS' : status }

        results = self.dbi.processData(sql, binds, conn = conn,
                                       transaction = transaction)[0].fetchall()

        requestList = []
        for result in results:
            request = {}
            request['id'] = result[0]
            request['input_dataset'] = result[1]
            request['run_id'] = result[2]
            request['min_lumi'] = result[3]
            request['max_lumi'] = result[4]
            request['global_tag'] = result[5]
            request['cmssw_version'] = result[6]
            request['scram_arch'] = result[7]
            request['proc_version'] = result[8]
            request['scenario'] = result[9]
            request['reco_split'] = result[10]
            request['multicore'] = result[11]
            request['write_reco'] = result[12]
            request['write_aod'] = result[13]
            request['write_miniaod'] = result[14]
            request['write_nanoaod'] = result[15]
            request['write_dqm'] = result[16]
            request['alca_skim'] = result[17]
            request['physics_skim'] = result[18]
            request['dqm_seq'] = result[19]
            request['nano_flavour'] = result[20]
            request['status'] = result[21]
            request['rucio_rule_id'] = result[22]
            request['workflow_name'] = result[23]
            request['fileset_id'] = result[24]
            request['error_message'] = result[25]
            request['created_at'] = result[26]
            request['updated_at'] = result[27]
            requestList.append(request)

        return requestList
